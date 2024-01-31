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

/*
Workers - array of connected workers.
*/
type Workers struct {
	mutex        sync.RWMutex
	workers      map[string]*Worker
	commonWorker *CommonWorker
}

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

	// The checking compatability of the share and the extensions of the worker.
	wRoll, wIsRoll := req.workerExtensions["version-rolling"]
	pRoll, pIsRoll := cw.cwExtensions["version-rolling"]
	if isRoll && (!wIsRoll || !wRoll.(bool)) {
		LogError("ignore share from miner without version rolling", "COMMON POOL")
		return
	}
	if isRoll && (!pIsRoll || !pRoll.(bool)) {
		LogError("ignore share to pool without version rolling", "COMMON POOL")
		return
	}
	if !isRoll && (wIsRoll && wRoll.(bool)) {
		LogError("ignore share from miner with version rolling", "COMMON POOL")
		return
	}
	if !isRoll && (pIsRoll && pRoll.(bool)) {
		LogError("ignore share to pool with version rolling", "COMMON POOL")
		return
	}

	req.params[0] = cw.cwUserName
	err := cw.client.Call("mining.submit", req.params, nil)
	if err != nil {
		LogError("ERROR WHILE SUBMIT TO COMMON POOL", "COMMON POOL")
	}
}

func (w *Workers) InitCommonWorker(addr, login, password string) error {

	// Connecting to the pool.
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot connect to common pool: %s", err.Error()))
	}

	client := rpc2.NewClientWithCodec(stratumrpc.NewStratumCodec(conn))
	cw := CommonWorker{client: client, workers: w.workers, cwUserName: login, poolAddr: addr, receiver: make(chan CommonWorkSubmit), canReceiveNewJob: true}

	client.Handle("mining.notify", cw.handleNotify)
	client.Handle("mining.set_difficulty", cw.handleSetDifficulty)

	go client.Run()

	// Sending authorize command to the pool.
	msgAuth := MiningAuthorizeRequest{login, password}
	params, err := msgAuth.Encode()
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot authorize %s by %s: %s", addr, login, err.Error()))
	}

	var breply bool
	err = client.Call("mining.authorize", params, &breply)
	if err != nil {
		return fmt.Errorf("got error while call authorize for common method: %s", err)
	}

	if !breply {
		return errors.New("access to the common pool denied")
	}

	w.mutex.Lock()
	w.commonWorker = &cw
	w.mutex.Unlock()

	return nil
}

func (w *Workers) add(worker *Worker) bool {
	id := worker.GetID()

	if wr := w.get(id); wr == nil {
		worker.mutex.Lock()
		worker.commonPoolResult = w.commonWorker.receiver
		worker.mutex.Unlock()

		w.workers[id] = worker
	} else {
		return false
	}

	return true
}

func (w *Workers) get(id string) *Worker {
	if worker, ok := w.workers[id]; ok {
		return worker
	}

	return nil
}

func (w *Workers) remove(id string) {
	if worker := w.get(id); worker != nil {
		delete(w.workers, id)
	}

	return
}

/*
Init - init of array.
*/
func (w *Workers) Init() {
	w.mutex.Lock()
	w.workers = make(map[string]*Worker)
	w.mutex.Unlock()
}

/*
Add - adding worker to array.

@param *Worker worker pointer to worker.

@return error
*/
func (w *Workers) Add(worker *Worker) error {
	wid := worker.GetID()

	if !ValidateHexString(wid) {
		return fmt.Errorf("invalid format worker.id = %s on add to workers table", wid)
	}

	w.mutex.Lock()
	result := w.add(worker)
	w.mutex.Unlock()

	if !result {
		return fmt.Errorf("worker.id = %s already exist in workers table", wid)
	}

	return nil
}

/*
Get - getting worker by his id.

@param string id - id of worker.

@return *Worker pointer to founded worker.

	error
*/
func (w *Workers) Get(id string) (*Worker, error) {
	if !ValidateHexString(id) {
		return nil, fmt.Errorf("invalid format id = %s on get from workers table", id)
	}

	w.mutex.RLock()
	worker := w.get(id)
	w.mutex.RUnlock()

	if worker == nil {
		return nil, fmt.Errorf("worker with id = %s not found in workers table", id)
	}

	return worker, nil
}

/*
Remove - remove worker from array.

@param string id -id of worker.

@return error
*/
func (w *Workers) Remove(id string) error {
	if !ValidateHexString(id) {
		return fmt.Errorf("invalid format id = %s on remove worker from workers table", id)
	}

	w.mutex.Lock()
	w.remove(id)
	w.mutex.Unlock()

	return nil
}
