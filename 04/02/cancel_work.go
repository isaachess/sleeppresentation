package main

import (
	"context"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type Worker struct {
	numWorkers int
	db         database
	valCh      chan [2]string
	eg         *errgroup.Group

	mu           sync.Mutex
	ongoingSaves map[string]*Retrier
}

func NewWorker(eg *errgroup.Group, db database, numWorkers int) *Worker {
	return &Worker{
		db:         db,
		valCh:      make(chan [2]string),
		numWorkers: numWorkers,
		eg:         eg,
	}
}

func (w *Worker) Start(ctx context.Context) {
	for i := 0; i < w.numWorkers; i++ {
		w.eg.Go(func() error {
			for {
				select {
				case pair := <-w.valCh:
					key, val := pair[0], pair[1]
					retrier := NewRetrier(5)

					w.mu.Lock()
					oldRetrier, ok := w.ongoingSaves[key]
					w.ongoingSaves[key] = retrier
					w.mu.Unlock()

					if ok {
						oldRetrier.Cancel()
					}

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

func (w *Worker) NotifyValue(key, val string) {
	w.valCh <- [2]string{key, val}
}

type Retrier struct {
	maxRetries int
	doneCh     chan struct{}
	wg         sync.WaitGroup
}

func NewRetrier(maxRetries int) *Retrier {
	var wg sync.WaitGroup
	wg.Add(1)
	return &Retrier{
		maxRetries: maxRetries,
		doneCh:     make(chan struct{}),
		wg:         wg,
	}
}

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

func (rs *Retrier) Cancel() {
	close(rs.doneCh)
	rs.wg.Wait()
}

type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}
