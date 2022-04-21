package main

import "context"

// Worker creates numWorks number of goroutines which read key/val pairs from
// the valCh and stores in the db.
type Worker struct {
	numWorkers int
	db         database
	valCh      chan [2]string
}

// NewWorker creates a new Worker.
func NewWorker(db database, numWorkers int) *Worker {
	return &Worker{
		db:         db,
		valCh:      make(chan [2]string),
		numWorkers: numWorkers,
	}
}

// Start is a non-blocking call that creates worker goroutines. They will exit
// when ctx is canceled.
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

// NotifyValue will queue the key/val pair to be stored in the db by the
// worker goroutines.
func (w *Worker) NotifyValue(key, val string) {
	w.valCh <- [2]string{key, val}
}

// database is a simple interface representing a database.
type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}
