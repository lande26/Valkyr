package store

import (
	"container/heap"
	"sync"
	"time"
)

// TTLStore manages key expiration using a min-heap and a background sweep goroutine.
// It calls deleteFunc to remove expired keys from the data stores.
type TTLStore struct {
	mu         sync.Mutex
	h          expiryHeap
	deadlines  map[string]int64 // key → unix-millis deadline
	deleteFunc func(string)     // callback to delete from data stores
	stopCh     chan struct{}
}

// NewTTLStore creates a new TTLStore with the given delete callback.
// The background sweeper is NOT started by this constructor — call StartSweeper separately.
func NewTTLStore(deleteFunc func(string)) *TTLStore {
	t := &TTLStore{
		deadlines:  make(map[string]int64),
		deleteFunc: deleteFunc,
		stopCh:     make(chan struct{}),
	}
	heap.Init(&t.h)
	return t
}

// StartSweeper starts the background goroutine that checks for expired keys every 100ms.
func (t *TTLStore) StartSweeper() {
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.sweep()
			case <-t.stopCh:
				return
			}
		}
	}()
}

// StopSweeper stops the background sweep goroutine.
func (t *TTLStore) StopSweeper() {
	close(t.stopCh)
}

// sweep checks the heap and removes all expired entries.
func (t *TTLStore) sweep() {
	now := time.Now().UnixMilli()
	t.mu.Lock()
	defer t.mu.Unlock()
	for t.h.Len() > 0 {
		top := t.h[0]
		if top.deadline > now {
			break
		}
		heap.Pop(&t.h)
		// Only delete if this is still the current deadline for this key
		// (the key might have been re-set with a new deadline)
		if d, ok := t.deadlines[top.key]; ok && d == top.deadline {
			delete(t.deadlines, top.key)
			// Call delete outside of our lock to avoid deadlock with store mutexes
			key := top.key
			t.mu.Unlock()
			t.deleteFunc(key)
			t.mu.Lock()
		}
	}
}

// SetExpire sets a TTL on a key in seconds from now. Returns true if the key's TTL was set.
func (t *TTLStore) SetExpire(key string, seconds int64) {
	deadline := time.Now().UnixMilli() + seconds*1000
	t.SetDeadline(key, deadline)
}

// SetExpireAt sets an absolute expiration time for a key (unix timestamp in seconds).
func (t *TTLStore) SetExpireAt(key string, unixSeconds int64) {
	deadline := unixSeconds * 1000
	t.SetDeadline(key, deadline)
}

// SetDeadline sets an absolute expiration deadline for a key (unix millis).
func (t *TTLStore) SetDeadline(key string, deadlineMs int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deadlines[key] = deadlineMs
	heap.Push(&t.h, expiryEntry{key: key, deadline: deadlineMs})
}

// GetTTL returns the remaining TTL in seconds for a key.
// Returns -1 if the key has no TTL, -2 is used by the caller when the key doesn't exist.
func (t *TTLStore) GetTTL(key string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	deadline, ok := t.deadlines[key]
	if !ok {
		return -1
	}
	remaining := (deadline - time.Now().UnixMilli()) / 1000
	if remaining < 0 {
		return 0
	}
	return remaining
}

// Remove removes the TTL for a key (PERSIST command). Returns true if the key had a TTL.
func (t *TTLStore) Remove(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.deadlines[key]
	if ok {
		delete(t.deadlines, key)
	}
	// We don't bother removing from the heap — the sweep will skip it
	// because the key won't be in deadlines anymore
	return ok
}

// GetDeadline returns the absolute deadline (unix millis) for a key.
func (t *TTLStore) GetDeadline(key string) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	d, ok := t.deadlines[key]
	return d, ok
}

// ExpiresCount returns the number of keys with TTLs set.
func (t *TTLStore) ExpiresCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.deadlines)
}

// Flush removes all TTL entries.
func (t *TTLStore) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deadlines = make(map[string]int64)
	t.h = expiryHeap{}
	heap.Init(&t.h)
}

// expiryEntry is an entry in the min-heap, ordered by deadline.
type expiryEntry struct {
	key      string
	deadline int64 // unix millis
}

// expiryHeap implements container/heap.Interface for TTL expiration.
type expiryHeap []expiryEntry

// Len returns the number of entries in the heap.
func (h expiryHeap) Len() int { return len(h) }

// Less reports whether element i should sort before element j (earlier deadline first).
func (h expiryHeap) Less(i, j int) bool { return h[i].deadline < h[j].deadline }

// Swap swaps two elements in the heap.
func (h expiryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap.
func (h *expiryHeap) Push(x interface{}) {
	*h = append(*h, x.(expiryEntry))
}

// Pop removes and returns the smallest element from the heap.
func (h *expiryHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	*h = old[:n-1]
	return entry
}
