/*
Stratum-proxy with external manage.
*/

package main

import (
	"flag"
	"fmt"
	"github.com/joho/godotenv"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"time"

	rpc2 "github.com/miningmeter/rpc2"
	"github.com/miningmeter/rpc2/stratumrpc"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"net/http"
)

/*
VERSION - proxy version.
*/
const VERSION = "0.01"

var (
	// Processing commangds from worker and pool.
	mining Mining
	// Workers.
	workers Workers
	// Db of users credentials.
	db Db
	// Out to syslog.
	syslog = false
	// GitCommit - Git commit for build
	GitCommit string
	// Compiled regexp for hexademical checks.
	rHexStr = regexp.MustCompile(`^[\da-fA-F]+$`)
	// Extensions that supported by the proxy.
	sExtensions = []string{
		"subscribe-extranonce",
		"version-rolling",
	}
	// SQLite db path.
	dbPath = "proxy.db"
	// Metrics proxy tag.
	tag = ""
)

// ProxyConfig main configuration fields of proxy that fetching from .env
type ProxyConfig struct {
	APIAddr   string
	ProxyAddr string

	// Info for common proxy connection
	CPSize     int
	CPAddr     string
	CPLogin    string
	CPPassword string
}

func fetchProxyConfig() (*ProxyConfig, error) {
	if err := godotenv.Load(); err != nil {
		return nil, fmt.Errorf("cannot load .env: %w", err)
	}

	// TODO: add validation of find
	apiAddr, apiFound := os.LookupEnv("API_ADDR")
	if !apiFound {
		return nil, fmt.Errorf("cannot find api addr")
	}

	proxyAddr, proxyFound := os.LookupEnv("PROXY_ADDR")
	if !proxyFound {
		return nil, fmt.Errorf("cannot find proxy addr")
	}

	var cpSize int
	var err error

	cpSizeStr, sizeFined := os.LookupEnv("CP_SIZE")
	if !sizeFined {
		cpSize = 0
	} else {
		cpSize, err = strconv.Atoi(cpSizeStr)
		if err != nil {
			return nil, fmt.Errorf("cannot convert CP_SIZE: %w", err)
		}
	}

	cpAddr, _ := os.LookupEnv("CP_ADDR")
	cpLogin, _ := os.LookupEnv("CP_LOGIN")
	cpPassword, _ := os.LookupEnv("CP_PASSWORD")

	return &ProxyConfig{
		APIAddr:   apiAddr,
		ProxyAddr: proxyAddr,

		CPSize:     cpSize,
		CPAddr:     cpAddr,
		CPLogin:    cpLogin,
		CPPassword: cpPassword,
	}, nil
}

/*
Main function.
*/
func main() {
	flag.BoolVar(&syslog, "syslog", false, "On true adapt log to out in syslog, hide date and colors")
	flag.StringVar(&dbPath, "db.path", "proxy.db", "Filepath for SQLite database")
	flag.StringVar(&tag, "metrics.tag", "127.0.0.1:8080", "Prometheus metrics proxy tag") // TODO
	flag.Parse()

	proxyConfig, err := fetchProxyConfig()
	if err != nil {
		panic(fmt.Errorf("cannot load proxy config: %w", err))
	}

	if syslog {
		log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	}
	LogInfo("proxy : version: %s-%s", "", VERSION, GitCommit)

	// Initializing of database.
	if !db.Init() {
		os.Exit(1)
	}
	defer db.Close()

	if proxyConfig.CPSize > 0 {
		err = workers.InitCommonWorker(proxyConfig.CPAddr, proxyConfig.CPLogin, proxyConfig.CPPassword)
		if err != nil {
			panic(fmt.Errorf("cannot start common worker: %w", err))
		}
	}

	// Inintializing of internal storage.
	workers.Init()

	// Initializing of API and metrics.
	LogInfo("proxy : web server serve on: %s", "", proxyConfig.APIAddr)
	// Users.
	http.Handle("/api/v1/users", &API{})
	// Metrics.
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(proxyConfig.APIAddr, nil)

	InitWorkerServer(proxyConfig.ProxyAddr)

	os.Exit(0)
}

/*
InitWorkerServer - initializing of server for workers connects.
*/
func InitWorkerServer(stratumAddr string) {
	// Launching of JSON-RPC server.
	server := rpc2.NewServer()
	// Subscribing of server to needed handlers.
	server.Handle("mining.subscribe", mining.Subscribe)
	server.Handle("mining.authorize", mining.Authorize)
	server.Handle("mining.submit", mining.Submit)
	server.Handle("mining.extranonce.subscribe", mining.ExtranonceSubscribe)
	server.Handle("mining.configure", mining.Configure)

	server.OnDisconnect(Disconnect)

	LogInfo("proxy : listen on: %s", "", stratumAddr)

	// Waiting of connections.
	link, _ := net.Listen("tcp", stratumAddr)
	for {
		conn, err := link.Accept()
		if err != nil {
			LogError("proxy : accept error: %s", "", err.Error())
			break
		}

		go WaitWorker(conn, server)
	}
}

/*
WaitWorker - waiting of worker init.

@param net.Conn     conn   - connection.
@param *rpc2.Server server - server.
*/
func WaitWorker(conn net.Conn, server *rpc2.Server) {
	addr := conn.RemoteAddr().String()
	LogInfo("%s : try connect to proxy", "", addr)
	// Initializing of worker.
	w := &Worker{addr: addr}
	// Linking of JSON-RPC connection to worker.
	state := rpc2.NewState()
	state.Set("worker", w)
	// Running of connection handler in goroutine.
	go server.ServeCodecWithState(stratumrpc.NewStratumCodec(conn), state)
	// Waiting 3 seconds of worker initializing, which will begin when the worker sends the commands.
	<-time.After(3 * time.Second)
	// If worker not initialized, we kill connection.
	if w.GetID() == "" {
		LogInfo("%s : disconnect by silence", "", addr)
		conn.Close()
	}
}

/*
Connect - processing of connecting worker to proxy.

@param *rpc2.Client client pointer to connecting client
@param *Worker w pointer to connecting worker
*/
func Connect(client *rpc2.Client, w *Worker) {
	wAddr := w.GetAddr()
	if err := w.Init(client); err == nil {
		sID := w.GetID()
		LogInfo("%s : connect to proxy", sID, wAddr)
	} else {
		LogError("%s : error connect to proxy: %s", "", wAddr, err.Error())
		client.Close()
	}
}

/*
Disconnect - processing of disconnecting worker to proxy.

@param *rpc2.Client client pointer to disconnecting client
*/
func Disconnect(client *rpc2.Client) {
	temp, _ := client.State.Get("worker")
	w := temp.(*Worker)
	w.Disconnect()
}
