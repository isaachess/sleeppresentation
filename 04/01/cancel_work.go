package main

import (
	"context"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// Worker creates numWorks number of goroutines which read key/val pairs from
// the valCh and stores in the db.
type Worker struct {
	numWorkers int
	db         database
	valCh      chan [2]string
	eg         *errgroup.Group

	mu           sync.Mutex
	ongoingSaves map[string]func()
}

// NewWorker creates a new Worker.
func NewWorker(eg *errgroup.Group, db database, numWorkers int) *Worker {
	return &Worker{
		db:         db,
		valCh:      make(chan [2]string),
		numWorkers: numWorkers,
		eg:         eg,
	}
}

// Start is a non-blocking call that creates worker goroutines. They will exit
// when ctx is canceled.
func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.numWorkers; i++ {
		w.eg.Go(func() error {
			for {
				select {
				case pair := <-w.valCh:
					ctx, cancel := context.WithCancel(ctx)
					key, val := pair[0], pair[1]

					// Check if an ongoing save is occurring for this key.
					w.mu.Lock()
					cancelFunc, ok := w.ongoingSaves[key]
					w.ongoingSaves[key] = cancel
					w.mu.Unlock()

					// If found, cancel the ongoing retry save.
					if ok {
						cancelFunc()
						// What do we do here?? We want to wait for other saves
						// for this key to cancel.
						time.Sleep(100 * time.Millisecond)
					}

					// Start this retry save after the old has been canceled.
					err := saveWithRetry(key, val, w.db, ctx.Done(), 5)
					if err != nil {
						log.Printf("failed to save with retry: %s", err)
					}
				case <-ctx.Done():
					return nil
				}
			}
		})
	}
}

// NotifyValue will queue the key/val pair to be stored in the db by the
// worker goroutines.
func (w *Worker) NotifyValue(key, val string) {
	w.valCh <- [2]string{key, val}
}

// saveWithRetry will attempt to save to the db multiple times. It will exit on
// successful save, after maxRetries, or if the doneCh is closed.
func saveWithRetry(key, val string, db database, doneCh <-chan struct{}, maxRetries int) (err error) {
	for i := 0; i < maxRetries; i++ {
		err = db.Save(key, val)
		if err == nil {
			return nil
		}
		select {
		case <-doneCh:
			return nil
		case <-time.After(time.Second):
		}
	}
	return err
}

// database is a simple interface representing a database.
type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}
