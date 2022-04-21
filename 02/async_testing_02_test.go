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

// testDB_02 fulfills the database interface, but allows for signaling when an
// expected number of saves has completed.
type testDB_02 struct {
	cache         sync.Map
	expectedSaves int

	mu    sync.Mutex
	saves int

	once   sync.Once
	notify chan struct{}
}

// NewTestDB_02 creates a new testDB_02, set up for the expected number of saves
// provided.
func NewTestDB_02(expectedSaves int) *testDB_02 {
	return &testDB_02{
		expectedSaves: expectedSaves,
		notify:        make(chan struct{}),
	}
}

// Save stores the key/val pair in the map. If the number of saves >= expected,
// it signals that the saves have been received.
func (tdb *testDB_02) Save(key, val string) error {
	tdb.cache.Store(key, val)

	tdb.mu.Lock()
	tdb.saves++
	if tdb.saves >= tdb.expectedSaves {
		tdb.once.Do(func() { close(tdb.notify) })
	}
	tdb.mu.Unlock()
	return nil
}

// Get loads and returns the value stored at key.
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

// Wait will block until the expected number of saves has been reached.
func (tdb *testDB_02) Wait(ctx context.Context) {
	select {
	case <-tdb.notify:
		return
	case <-ctx.Done():
		return
	}
}
