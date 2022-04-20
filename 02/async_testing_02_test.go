package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotifyValue_02(t *testing.T) {
	ctx := context.Background()

	pairs := [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}

	db := NewTestDB_02(len(pairs))
	worker := NewWorker(db, 5)
	worker.Start(ctx)

	for _, pair := range pairs {
		worker.NotifyValue(pair[0], pair[1])
	}

	// Now we wait for the exact time we need.
	db.Wait(ctx)

	// Verify they have been saved properly.
	for _, pair := range pairs {
		fromDB, err := db.Get(pair[0])
		require.NoError(t, err)
		require.Equal(t, pair[1], fromDB)
	}
}

type testDB_02 struct {
	cache         sync.Map
	expectedSaves int

	mu     sync.Mutex
	saves  int
	notify chan struct{}
}

func NewTestDB_02(expectedSaves int) *testDB_02 {
	return &testDB_02{
		expectedSaves: expectedSaves,
		notify:        make(chan struct{}),
	}
}

func (tdb *testDB_02) Save(key, val string) error {
	tdb.cache.Store(key, val)

	tdb.mu.Lock()
	tdb.saves++
	if tdb.saves >= tdb.expectedSaves {
		close(tdb.notify)
	}
	tdb.mu.Unlock()
	return nil
}

func (tdb *testDB_02) Get(key string) (string, error) {
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

func (tdb *testDB_02) Wait(ctx context.Context) {
	select {
	case <-tdb.notify:
		return
	case <-ctx.Done():
		return
	}
}
