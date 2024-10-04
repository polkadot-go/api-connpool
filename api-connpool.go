package connpool

import (
	"errors"
	"fmt"
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
}

// New initializes the ConnectionPool with general configuration
func New(loadbalancing string, connectionTimeout, queryTimeout time.Duration) *ConnectionPool {
	return &ConnectionPool{
		loadbalancing:     loadbalancing,
		connectionTimeout: connectionTimeout,
		queryTimeout:      queryTimeout,
		rpcServers:        make([]*RPCServer, 0),
		roundRobinIndex:   0,
	}
}

// checkChainName checks if the chain name of the provided API matches all other servers
func (cp *ConnectionPool) checkChainName(api *gsrpc.SubstrateAPI) bool {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	chainName, err := api.RPC.System.Chain()
	if err != nil {
		return false
	}

	if cp.chainName == "" {
		// Set the chain name for the pool if not already set
		cp.chainName = string(chainName)
		return true
	}

	// Ensure chain name matches the existing pool's chain name
	return chainName == types.Text(cp.chainName)
}

// AddRPC adds a new RPC server to the connection pool
func (cp *ConnectionPool) AddRPC(uid, name, url string, maxConnections, maxThreads, numRetries, maxFailures int) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	connections := make([]*RPCConnection, maxConnections)
	for i := 0; i < maxConnections; i++ {
		api, err := gsrpc.NewSubstrateAPI(url)
		if err != nil {
			return
		}
		if i == 0 {
			// Check chain name
			if !cp.checkChainName(api) {
				newServer := &RPCServer{
					UID:      uid,
					Name:     name,
					URL:      url,
					isActive: false,
				}
				cp.rpcServers = append(cp.rpcServers, newServer)
				return
			}
		}
		connections[i] = &RPCConnection{
			api:            api,
			activeQueries:  0,
			totalQueries:   0,
			activeFailures: 0,
			totalFailures:  0,
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
}

// DelRPC deletes an RPC server from the connection pool
func (cp *ConnectionPool) DelRPC(uid string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

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
						conn.mutex.Unlock()
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
			return leastUsedServer, leastUsedConn, nil
		}
	}

	return nil, nil, errors.New("no valid RPC connection available in the connection pool based on load balancing strategy")
}

// RPC is a wrapper to execute RPC functions from gsrpc through the connection pool
func (cp *ConnectionPool) RPC(fn func(*gsrpc.SubstrateAPI) error) ([]string, error) {
	server, conn, err := cp.GetConnection()
	if err != nil {
		return nil, err
	}

	conn.mutex.Lock()
	conn.activeQueries++
	conn.totalQueries++
	conn.mutex.Unlock()

	server.mutex.Lock()
	server.totalQueries++
	server.mutex.Unlock()

	startTime := time.Now()

	defer func() {
		conn.mutex.Lock()
		conn.activeQueries--
		conn.mutex.Unlock()
	}()

	// Run the requested function with retry logic
	for i := 0; i < server.NumRetries; i++ {
		err = fn(conn.api)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second) // Delay between retries
	}

	// Track query time
	elapsedTime := time.Since(startTime)

	// Handle failure tracking
	conn.mutex.Lock()
	if err != nil {
		conn.activeFailures++
		conn.totalFailures++
	}
	conn.mutex.Unlock()

	server.mutex.Lock()
	totalFailures := 0
	for _, connection := range server.connections {
		connection.mutex.Lock()
		totalFailures += connection.activeFailures
		connection.mutex.Unlock()
	}
	if totalFailures > server.MaxFailures {
		server.isActive = false // Mark server as offline
	}
	server.totalFailures = totalFailures
	server.mutex.Unlock()

	// Prepare the query details for return
	queryDetails := []string{
		fmt.Sprintf("Server UID: %s, Server Name: %s, URL: %s", server.UID, server.Name, server.URL),
		fmt.Sprintf("Query Time: %v", elapsedTime),
		fmt.Sprintf("Total Queries: %d, Total Failures: %d", server.totalQueries, server.totalFailures),
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
					}
				}
			}
		}
		cp.mutex.Unlock()
	}
}

// Example function to use the connection pool
type Chain struct {
	pool *ConnectionPool
}

func (c *Chain) GetHeaderLatest() (*types.Header, []string, error) {
	var header *types.Header
	queryDetails, err := c.pool.RPC(func(api *gsrpc.SubstrateAPI) error {
		response, err := api.RPC.Chain.GetHeaderLatest()
		if err != nil {
			return err
		}
		header = response
		return nil
	})

	return header, queryDetails, err
}
