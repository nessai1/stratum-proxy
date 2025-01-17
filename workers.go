package main

import (
	"fmt"
	"sync"
)

/*
Workers - array of connected workers.
*/
type Workers struct {
	mutex        sync.RWMutex
	workers      map[string]*Worker
	commonWorker *CommonWorker
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
