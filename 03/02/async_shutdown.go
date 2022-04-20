package main

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type database interface {
	Save(key, val string) error
	Get(key string) (val string, err error)
}

type Worker struct {
	numWorkers int
	db         database
	valCh      chan [2]string
	eg         *errgroup.Group
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
					w.db.Save(pair[0], pair[1])
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
