package connpool

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	gsrpc "github.com/polkadot-go/api/v4"
	types "github.com/polkadot-go/api/v4/types"
)

type RPCConnection struct {
	api            *gsrpc.SubstrateAPI
	activeQueries  int
	totalQueries   int
	activeFailures int
	totalFailures  int
	mutex          sync.Mutex
}

type RPCServer struct {
	UID            string
	Name           string
	URL            string
	MaxConnections int
	MaxThreads     int
	NumRetries     int
	MaxFailures    int
	connections    []*RPCConnection
	totalQueries   int
	totalFailures  int
	isActive       bool
	chainName      string
	mutex          sync.Mutex
}

type ConnectionPool struct {
	rpcServers        []*RPCServer
	loadbalancing     string
	connectionTimeout time.Duration
	queryTimeout      time.Duration
	mutex             sync.Mutex
	roundRobinIndex   int
	chainName         string
	debug             bool
	bufferedQueries   chan func()
}

// New initializes the ConnectionPool with general configuration and a debug flag
func New(loadbalancing string, connectionTimeout, queryTimeout time.Duration, debug bool) *ConnectionPool {
	cp := &ConnectionPool{
		loadbalancing:     loadbalancing,
		connectionTimeout: connectionTimeout,
		queryTimeout:      queryTimeout,
		rpcServers:        make([]*RPCServer, 0),
		roundRobinIndex:   0,
		debug:             debug,
		bufferedQueries:   make(chan func(), 1000), // Buffer size for queries
	}
	// Launch CheckInactiveServers in a separate goroutine
	go cp.CheckInactiveServers()
	// Launch worker to process buffered queries
	go cp.processBufferedQueries()
	return cp
}

// logDebug logs messages if debugging is enabled
func (cp *ConnectionPool) logDebug(message string) {
	if cp.debug {
		log.Println("[DEBUG]", message)
	}
}

// checkChainName checks if the chain name of the provided API matches all other servers
func (cp *ConnectionPool) checkChainName(api *gsrpc.SubstrateAPI) bool {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	chainName, err := api.RPC.System.Chain()
	if err != nil {
		cp.logDebug(fmt.Sprintf("Failed to get chain name: %v", err))
		return false
	}

	cp.logDebug(fmt.Sprintf("Retrieved chain name: %s", chainName))

	if cp.chainName == "" {
		// Set the chain name for the pool if not already set
		cp.chainName = string(chainName)
		cp.logDebug(fmt.Sprintf("Setting chain name to: %s", chainName))
		return true
	}

	// Ensure chain name matches the existing pool's chain name
	isMatch := chainName == types.Text(cp.chainName)
	if !isMatch {
		cp.logDebug(fmt.Sprintf("Chain name mismatch: expected %s, got %s", cp.chainName, chainName))
	}
	return isMatch
}

// AddRPC adds a new RPC server to the connection pool
func (cp *ConnectionPool) AddRPC(uid, name, url string, maxConnections, maxThreads, numRetries, maxFailures int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.logDebug(fmt.Sprintf("Attempting to add RPC server: UID=%s, Name=%s, URL=%s", uid, name, url))

	connections := make([]*RPCConnection, maxConnections)
	for i := 0; i < maxConnections; i++ {
		var api *gsrpc.SubstrateAPI
		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			cp.logDebug(fmt.Sprintf("Establishing connection %d/%d for server %s (attempt %d)", i+1, maxConnections, uid, attempt))
			api, err = gsrpc.NewSubstrateAPI(url)
			if err == nil {
				break
			}
			cp.logDebug(fmt.Sprintf("Failed to create API connection for %s (connection %d, attempt %d): %v", url, i+1, attempt, err))
			time.Sleep(2 * time.Second) // Retry delay
		}
		if err != nil {
			cp.logDebug(fmt.Sprintf("Exiting AddRPC due to failure in creating API connection after 3 attempts for %s (connection %d). Error: %v", url, i+1, err))
			return
		}
		connections[i] = &RPCConnection{
			api:            api,
			activeQueries:  0,
			totalQueries:   0,
			activeFailures: 0,
			totalFailures:  0,
		}
		cp.logDebug(fmt.Sprintf("Successfully established connection %d/%d for server %s", i+1, maxConnections, uid))
	}

	// Check chain name only if more than one server is defined
	if len(cp.rpcServers) > 1 {
		if !cp.checkChainName(connections[0].api) {
			newServer := &RPCServer{
				UID:      uid,
				Name:     name,
				URL:      url,
				isActive: false,
			}
			cp.rpcServers = append(cp.rpcServers, newServer)
			cp.logDebug(fmt.Sprintf("Chain name mismatch. Marking server %s as inactive.", uid))
			return
		}
	}

	newServer := &RPCServer{
		UID:            uid,
		Name:           name,
		URL:            url,
		MaxConnections: maxConnections,
		MaxThreads:     maxThreads,
		NumRetries:     numRetries,
		MaxFailures:    maxFailures,
		connections:    connections,
		totalQueries:   0,
		totalFailures:  0,
		isActive:       true,
		chainName:      cp.chainName,
	}

	cp.rpcServers = append(cp.rpcServers, newServer)
	cp.logDebug(fmt.Sprintf("Successfully added RPC server: UID=%s, Name=%s, URL=%s", uid, name, url))
}

// DelRPC deletes an RPC server from the connection pool
func (cp *ConnectionPool) DelRPC(uid string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	cp.logDebug(fmt.Sprintf("Deleting RPC server: UID=%s", uid))

	for i, server := range cp.rpcServers {
		if server.UID == uid {
			// Wait for all active queries to finish
			server.mutex.Lock()
			canDelete := true
			for _, conn := range server.connections {
				conn.mutex.Lock()
				if conn.activeQueries > 0 {
					canDelete = false
				}
				conn.mutex.Unlock()
			}
			if canDelete {
				cp.rpcServers = append(cp.rpcServers[:i], cp.rpcServers[i+1:]...)
				cp.logDebug(fmt.Sprintf("Successfully deleted RPC server: UID=%s", uid))
			}
			server.mutex.Unlock()
			break
		}
	}
}

// GetConnection retrieves an RPC connection based on load balancing strategy
func (cp *ConnectionPool) GetConnection() (*RPCServer, *RPCConnection, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if len(cp.rpcServers) == 0 {
		cp.logDebug("No RPC servers available in the connection pool.")
		return nil, nil, errors.New("no RPC servers available in the connection pool")
	}

	// Implementing round-robin or least-used strategies
	if cp.loadbalancing == "round_robin" {
		// Round-robin: rotate between servers
		for i := 0; i < len(cp.rpcServers); i++ {
			server := cp.rpcServers[cp.roundRobinIndex]
			cp.roundRobinIndex = (cp.roundRobinIndex + 1) % len(cp.rpcServers)
			if server.isActive {
				for _, conn := range server.connections {
					conn.mutex.Lock()
					if conn.activeQueries < server.MaxThreads {
						conn.activeQueries++
						conn.mutex.Unlock()
						cp.logDebug(fmt.Sprintf("Using RPC server: UID=%s, Name=%s", server.UID, server.Name))
						return server, conn, nil
					}
					conn.mutex.Unlock()
				}
			}
		}
	} else if cp.loadbalancing == "least_used" {
		// Pick connection with the least active queries
		var leastUsedServer *RPCServer
		var leastUsedConn *RPCConnection
		for _, server := range cp.rpcServers {
			if server.isActive {
				for _, conn := range server.connections {
					conn.mutex.Lock()
					if leastUsedConn == nil || conn.activeQueries < leastUsedConn.activeQueries {
						leastUsedServer = server
						leastUsedConn = conn
					}
					conn.mutex.Unlock()
				}
			}
		}
		if leastUsedConn != nil {
			leastUsedConn.mutex.Lock()
			leastUsedConn.activeQueries++
			leastUsedConn.mutex.Unlock()
			cp.logDebug(fmt.Sprintf("Using least used RPC server: UID=%s, Name=%s", leastUsedServer.UID, leastUsedServer.Name))
			return leastUsedServer, leastUsedConn, nil
		}
	}

	cp.logDebug("No valid RPC connection available in the connection pool based on load balancing strategy.")
	return nil, nil, errors.New("no valid RPC connection available in the connection pool based on load balancing strategy")
}

// processBufferedQueries processes buffered queries when connections become available
func (cp *ConnectionPool) processBufferedQueries() {
	for query := range cp.bufferedQueries {
		query()
	}
}

// RPC method allows executing a provided function using an active RPC connection
func (cp *ConnectionPool) RPC(fn func(api *gsrpc.SubstrateAPI) error) ([]string, error) {
	server, conn, err := cp.GetConnection()
	if err != nil {
		// Buffer the query to be retried later
		cp.bufferedQueries <- func() {
			_, _ = cp.RPC(fn) // Reattempt the query
		}
		return nil, err
	}

	conn.mutex.Lock()
	conn.totalQueries++
	conn.mutex.Unlock()

	start := time.Now()
	err = fn(conn.api)
	duration := time.Since(start)

	conn.mutex.Lock()
	conn.activeQueries--
	if err != nil {
		conn.activeFailures++
		server.mutex.Lock()
		server.totalFailures++
		server.mutex.Unlock()
	}
	conn.mutex.Unlock()

	queryDetails := []string{
		fmt.Sprintf("Server UID: %s, Name: %s, URL: %s", server.UID, server.Name, server.URL),
		fmt.Sprintf("Query Duration: %v", duration),
	}

	return queryDetails, err
}

// CheckInactiveServers checks inactive servers and attempts to reinstate them if they are functioning correctly
func (cp *ConnectionPool) CheckInactiveServers() {
	for {
		time.Sleep(60 * time.Minute)
		cp.mutex.Lock()
		for _, server := range cp.rpcServers {
			if !server.isActive {
				api, err := gsrpc.NewSubstrateAPI(server.URL)
				if err == nil {
					// Recheck chain name to ensure it matches
					if cp.checkChainName(api) {
						server.mutex.Lock()
						for _, conn := range server.connections {
							conn.api = api
							conn.activeFailures = 0
						}
						server.isActive = true
						server.totalFailures = 0
						server.MaxConnections = int(float64(server.MaxConnections) * 0.9)
						server.MaxThreads = int(float64(server.MaxThreads) * 0.9)
						server.mutex.Unlock()
						cp.logDebug(fmt.Sprintf("Reinstated inactive server: UID=%s, Name=%s", server.UID, server.Name))
					}
				}
			}
		}
		cp.mutex.Unlock()
	}
}
