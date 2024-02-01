package main

import (
	"errors"
	"fmt"
	"github.com/miningmeter/rpc2"
	"github.com/miningmeter/rpc2/stratumrpc"
	"net"
	"sync"
	"time"
)

type CommonWorkSubmit struct {
	params           []interface{}
	submitRequest    MiningSubmitRequest
	workerExtensions map[string]interface{}
}

type CommonWorker struct {
	mutex sync.RWMutex

	poolDifficult []interface{}

	canReceiveNewJob bool

	poolAddr     string
	cwUserName   string
	cwExtensions map[string]interface{}

	client *rpc2.Client

	receiver chan CommonWorkSubmit

	workers map[string]*Worker
}

func NewCommonWorker(addr, login, password string, workers map[string]*Worker) (*CommonWorker, error) {

	cw, err := initCommonWorker(addr, login, password, workers)
	if err != nil {
		return nil, fmt.Errorf("cannot init common worker: %w", err)
	}

	var breply bool
	var areply interface{}

	configureRequest := MiningConfigureRequest{
		extensions: map[string]interface{}{
			"version-rolling":               true,
			"subscribe-extranonce":          true,
			"version-rolling.mask":          "1fffe000",
			"version-rolling.min-bit-count": 16,
		},
	}

	configureParams, err := configureRequest.Encode()
	if err != nil {
		return nil, fmt.Errorf("cannot encode configure request: %w", err)
	}

	err = cw.client.Call("mining.configure", configureParams, &areply)
	if err != nil {
		return nil, fmt.Errorf("cannot send conf request: %w", err)
	} else {
		LogInfo("common pool configured!", "COMMON POOL")
	}

	re := new(MiningConfigureResponse)
	err = re.Decode(areply)
	if err != nil {
		return nil, fmt.Errorf("cannot decode reply from configure: %w", err)
	}

	for key, val := range re.extensions {
		v, ok := val.(string)
		if ok {
			LogInfo("CONFIGURE RESPONSE KEY: %s, VAL: %s", "COMMON POOL", key, v)
		}

		vv, ok := val.(bool)
		if ok {
			LogInfo("CONFIGURE RESPONSE KEY: %s VAL: %b", "COMMON POOL", key, vv)
		}
	}

	subscribeRequest := MiningSubscribeRequest{
		ua: "cgminer",
	}
	subscribeParams, err := subscribeRequest.Encode()
	if err != nil {
		return nil, fmt.Errorf("cannot encode subscribe request: %w", err)
	}

	err = cw.client.Call("mining.subscribe", subscribeParams, &areply)
	if err != nil {
		return nil, fmt.Errorf("cannot subscribe common pool: %w", err)
	} else {
		LogInfo("common pool subscribed!", "COMMON POOL")
	}
	//if !breply {
	//	return nil, errors.New("access to the common pool denied (subscribe)")
	//}

	// Sending authorize command to the pool.
	msgAuth := MiningAuthorizeRequest{login, password}
	params, err := msgAuth.Encode()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Cannot authorize %s by %s: %s", addr, login, err.Error()))
	}
	err = cw.client.Call("mining.authorize", params, &breply)
	if err != nil {
		return nil, fmt.Errorf("got error while call authorize for common method: %s", err)
	} else {
		LogInfo("common pool authorized!", "COMMON POOL")
	}

	if !breply {
		return nil, errors.New("access to the common pool denied (authorize)")
	}

	go cw.ListenJobs()

	return cw, nil
}

func initCommonWorker(addr, login, password string, workers map[string]*Worker) (*CommonWorker, error) {
	// Connecting to the pool.
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("Cannot connect to common pool: %s", err.Error()))
	}

	client := rpc2.NewClientWithCodec(stratumrpc.NewStratumCodec(conn))
	cw := CommonWorker{client: client, workers: workers, cwUserName: login, poolAddr: addr, receiver: make(chan CommonWorkSubmit), canReceiveNewJob: true}

	client.Handle("mining.notify", cw.handleNotify)
	client.Handle("mining.set_difficulty", cw.handleSetDifficulty)

	go client.Run()

	return &cw, nil
}

func (cw *CommonWorker) handleNotify(client *rpc2.Client, params []interface{}, res *interface{}) error {
	cw.mutex.Lock()
	canReceiveNewJob := cw.canReceiveNewJob
	difficult := cw.poolDifficult
	cw.mutex.Unlock()

	LogInfo("common pool got NOTIFY", "COMMON POOL")

	if !canReceiveNewJob {
		return nil
	}

	worker := cw.chooseWorker()
	if worker == nil {
		LogError("Cannot choose worker for common job (proxy have 0 workers)", "COMMON POOL")
		return nil
	}
	err := worker.PushCommonJob(params, difficult)
	if err != nil {
		LogError("Worker can't get new job: %s", "COMMON POOL", err.Error())
	} else {
		LogInfo("Worker %s was choose to make common work", "COMMON POOL", worker.GetID())
	}

	return nil
}

func (cw *CommonWorker) chooseWorker() *Worker {
	for _, val := range cw.workers {
		return val
	}

	return nil
}

func (cw *CommonWorker) handleSetDifficulty(client *rpc2.Client, params []interface{}, res *interface{}) error {
	cw.mutex.Lock()
	cw.poolDifficult = params
	cw.mutex.Unlock()

	return nil
}

func (cw *CommonWorker) ListenJobs() {
	for req := range cw.receiver {
		cw.handleJob(req)

		cw.mutex.Lock()
		cw.canReceiveNewJob = true
		cw.mutex.Unlock()
	}
}

func (cw *CommonWorker) handleJob(req CommonWorkSubmit) {
	isRoll := req.submitRequest.versionbits != ""

	if isRoll {
		LogInfo("%s > mining.submit (isRoll): %s, %s, %s", "COMMON POOL", cw.poolAddr, req.submitRequest.job, req.submitRequest.nonce, req.submitRequest.versionbits)
	} else {
		LogInfo("%s > mining.submit: %s, %s", "COMMON POOL", cw.poolAddr, req.submitRequest.job, req.submitRequest.nonce)
	}

	//// The checking compatability of the share and the extensions of the worker.
	//wRoll, wIsRoll := req.workerExtensions["version-rolling"]
	//pRoll, pIsRoll := cw.cwExtensions["version-rolling"]
	//if isRoll && (!wIsRoll || !wRoll.(bool)) {
	//	LogError("ignore share from miner without version rolling", "COMMON POOL")
	//	return
	//}
	//if isRoll && (!pIsRoll || !pRoll.(bool)) {
	//	LogError("ignore share to pool without version rolling", "COMMON POOL")
	//	return
	//}
	//if !isRoll && (wIsRoll && wRoll.(bool)) {
	//	LogError("ignore share from miner with version rolling", "COMMON POOL")
	//	return
	//}
	//if !isRoll && (pIsRoll && pRoll.(bool)) {
	//	LogError("ignore share to pool with version rolling", "COMMON POOL")
	//	return
	//}

	req.params[0] = cw.cwUserName
	err := cw.client.Call("mining.submit", req.params, nil)
	if err != nil {
		LogError("ERROR WHILE SUBMIT TO COMMON POOL: %s", "COMMON POOL", err.Error())
	}
}
