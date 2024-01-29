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

type CommonWorker struct {
	client *rpc2.Client
}

func (cw *CommonWorker) handleNotify(client *rpc2.Client, params []interface{}, res *interface{}) error {
	LogInfo("Got new notify", "COMMON_WORKER")
	return nil
}

func (cw *CommonWorker) handleSetDifficulty(client *rpc2.Client, params []interface{}, res *interface{}) error {
	LogInfo("Got new set difficulty", "COMMON_WORKER")
	return nil
}

func (w *Workers) add(worker *Worker) bool {
	id := worker.GetID()

	if wr := w.get(id); wr == nil {
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

func (w *Workers) InitCommonWorker(addr, login, password string) error {

	// Connecting to the pool.
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot connect to common pool: %s", err.Error()))
	}

	client := rpc2.NewClientWithCodec(stratumrpc.NewStratumCodec(conn))
	cw := CommonWorker{client: client}

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
