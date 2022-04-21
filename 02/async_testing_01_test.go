package main

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNotifyValue_01(t *testing.T) {
	ctx := context.Background()

	pairs := [][2]string{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}

	db := &testDB{}
	worker := NewWorker(db, 5)
	worker.Start(ctx)

	for _, pair := range pairs {
		worker.NotifyValue(pair[0], pair[1])
	}

	// What to do here??
	time.Sleep(100 * time.Millisecond)

	// Verify they have been saved properly.
	for _, pair := range pairs {
		fromDB, err := db.Get(pair[0])
		require.NoError(t, err)
		require.Equal(t, pair[1], fromDB)
	}
}

// testDB fulfills the database interface
type testDB struct {
	cache sync.Map
}

// Save simply stores the key/val pair in the map.
func (tdb *testDB) Save(key, val string) error {
	tdb.cache.Store(key, val)
	return nil
}

// Get loads and returns the value stored at key.
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
