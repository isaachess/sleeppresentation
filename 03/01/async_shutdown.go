package main

import "context"

type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}

type Worker struct {
	numWorkers int
	db         database
	valCh      chan [2]string
}

func NewWorker(db database, numWorkers int) *Worker {
	return &Worker{
		db:         db,
		valCh:      make(chan [2]string),
		numWorkers: numWorkers,
	}
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.numWorkers; i++ {
		go func() {
			for {
				select {
				case pair := <-w.valCh:
					w.db.Save(pair[0], pair[1])
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}

func (w *Worker) NotifyValue(key, val string) {
	w.valCh <- [2]string{key, val}
}
