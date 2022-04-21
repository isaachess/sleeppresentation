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
	ongoingSaves map[string]*Retrier
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
					key, val := pair[0], pair[1]
					retrier := NewRetrier(5)

					// Check if an ongoing save is occurring for this key.
					w.mu.Lock()
					oldRetrier, ok := w.ongoingSaves[key]
					w.ongoingSaves[key] = retrier
					w.mu.Unlock()

					// If found, cancel the ongoing retry save.
					if ok {
						oldRetrier.Cancel()
					}

					// Start this retry save after the old has been canceled.
					err := retrier.Retry(func() error {
						return w.db.Save(key, val)
					})
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

// Retrier is a struct that supports retrying operations.
type Retrier struct {
	maxRetries int
	doneCh     chan struct{}
	wg         sync.WaitGroup
}

// NewRetrier creates a new Retrier.
func NewRetrier(maxRetries int) *Retrier {
	var wg sync.WaitGroup
	wg.Add(1)
	return &Retrier{
		maxRetries: maxRetries,
		doneCh:     make(chan struct{}),
		wg:         wg,
	}
}

// Retry will execute f up to rs.maxRetries times. It will exit on success,
// after maxRetries, or if Cancel() is called.
func (rs *Retrier) Retry(f func() error) (err error) {
	defer rs.wg.Done()
	for i := 0; i < rs.maxRetries; i++ {
		err = f()
		if err == nil {
			return nil
		}
		select {
		case <-rs.doneCh:
			return nil
		case <-time.After(time.Second):
		}
	}
	return err
}

// Cancel signals the Retrier will stop execution. It will block until the Retry
// loop has successfully exited.
func (rs *Retrier) Cancel() {
	close(rs.doneCh)
	rs.wg.Wait()
}

// database is a simple interface representing a database.
type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}
