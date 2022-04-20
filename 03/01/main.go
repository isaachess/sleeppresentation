package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker(&testDB{}, 5)
	worker.Start(ctx)

	// Worker processes incoming notifications on NotifyValue...

	// It's time to shutdown.
	cancel()

	// What do we do here? We want to ensure all values are stored in the DB.
	time.Sleep(100 * time.Millisecond)

	return nil
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
