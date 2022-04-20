package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"golang.org/x/sync/errgroup"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	var eg errgroup.Group
	worker := NewWorker(&eg, &testDB{}, 5)
	worker.Start(ctx)
	// Worker processes incoming notifications on NotifyValue...
	cancel()

	// Wait for the goroutines to exit
	return eg.Wait()
}

type testDB struct {
	cache sync.Map
}

func (tdb *testDB) Save(key, val string) error {
	tdb.cache.Store(key, val)
	return nil
}

func (tdb *testDB) Get(key string) (string, error) {
	val, ok := tdb.cache.Load(key)
	if !ok {
		return "", fmt.Errorf("no key found for %s", key)
	}

	str, ok := val.(string)
	if !ok {
		return "", fmt.Errorf("unexpected type format for val")
	}
	return str, nil
}
