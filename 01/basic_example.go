package main

import "time"

// asyncCache is a cache that will set values asynchronously.
type asyncCache interface {
	// Set will return immediately, and asynchronously set the value.
	Set(key, val string)
	// Get will retrieve the value stored at key.
	Get(key string) (val string)
	// Size returns the total number of keys in the cache.
	Size() int
}

// Workeradds values to the cache.
type Worker struct {
	cache asyncCache
}

// SetValues will add a series of values to the cache and return the total
// number of items in the cache.
func (w *Worker) SetValues(pairs [][2]string) int {
	for _, pair := range pairs {
		w.cache.Set(pair[0], pair[1])
	}

	// What to do here??
	time.Sleep(100 * time.Millisecond)

	return w.cache.Size()
}
