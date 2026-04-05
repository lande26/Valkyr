// Package store implements the in-memory data stores for Valkyr.
// It provides a unified Store aggregate that holds all data type stores
// (strings, hashes, lists, sets, zsets) and TTL management.
package store

import (
	"math/rand"
	"runtime"
	"sync"
	"time"
)

// KeyMetadata stores access stats for LRU/LFU eviction.
type KeyMetadata struct {
	LastAccess  time.Time
	AccessCount int64
}

// Store is the top-level aggregate that holds all individual data stores.
// It is passed via constructor injection to the server and router — no global state.
type Store struct {
	Strings *StringStore
	Hashes  *HashStore
	Lists   *ListStore
	Sets    *SetStore
	ZSets   *ZSetStore
	TTL     *TTLStore

	metaMu sync.Mutex
	meta   map[string]*KeyMetadata
}

// NewStore creates a new Store with all sub-stores initialized.
// The deleteFunc on the TTL store is wired to remove keys from all data stores.
func NewStore() *Store {
	s := &Store{
		Strings: NewStringStore(),
		Hashes:  NewHashStore(),
		Lists:   NewListStore(),
		Sets:    NewSetStore(),
		ZSets:   NewZSetStore(),
		meta:    make(map[string]*KeyMetadata),
	}
	s.TTL = NewTTLStore(func(key string) {
		s.Strings.Delete(key)
		s.Hashes.Delete(key)
		s.Lists.Delete(key)
		s.Sets.Delete(key)
		s.ZSets.Delete(key)
		s.RemoveMeta(key)
	})
	return s
}

// Touch updates a key's access metadata.
func (s *Store) Touch(key string) {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	m, ok := s.meta[key]
	if !ok {
		m = &KeyMetadata{}
		s.meta[key] = m
	}
	m.LastAccess = time.Now()
	m.AccessCount++
}

// RemoveMeta deletes key access metadata.
func (s *Store) RemoveMeta(key string) {
	s.metaMu.Lock()
	delete(s.meta, key)
	s.metaMu.Unlock()
}

// KeyExists checks whether a key exists in any of the data stores.
func (s *Store) KeyExists(key string) bool {
	s.Touch(key)
	if s.Strings.Exists(key) {
		return true
	}
	if s.Hashes.Exists(key) {
		return true
	}
	if s.Lists.Exists(key) {
		return true
	}
	if s.Sets.Exists(key) {
		return true
	}
	if s.ZSets.Exists(key) {
		return true
	}
	return false
}

// KeyType returns the Redis type name of the key, or "none" if the key doesn't exist.
func (s *Store) KeyType(key string) string {
	s.Touch(key)
	if s.Strings.Exists(key) {
		return "string"
	}
	if s.Hashes.Exists(key) {
		return "hash"
	}
	if s.Lists.Exists(key) {
		return "list"
	}
	if s.Sets.Exists(key) {
		return "set"
	}
	if s.ZSets.Exists(key) {
		return "zset"
	}
	return "none"
}

// DeleteKey removes a key from whichever store holds it. Returns true if deleted.
func (s *Store) DeleteKey(key string) bool {
	s.RemoveMeta(key)
	if s.Strings.Delete(key) {
		s.TTL.Remove(key)
		return true
	}
	if s.Hashes.Delete(key) {
		s.TTL.Remove(key)
		return true
	}
	if s.Lists.Delete(key) {
		s.TTL.Remove(key)
		return true
	}
	if s.Sets.Delete(key) {
		s.TTL.Remove(key)
		return true
	}
	if s.ZSets.Delete(key) {
		s.TTL.Remove(key)
		return true
	}
	return false
}

// AllKeys returns all keys across all data stores.
func (s *Store) AllKeys() []string {
	seen := make(map[string]struct{})
	for _, k := range s.Strings.Keys() {
		seen[k] = struct{}{}
	}
	for _, k := range s.Hashes.Keys() {
		seen[k] = struct{}{}
	}
	for _, k := range s.Lists.Keys() {
		seen[k] = struct{}{}
	}
	for _, k := range s.Sets.Keys() {
		seen[k] = struct{}{}
	}
	for _, k := range s.ZSets.Keys() {
		seen[k] = struct{}{}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys
}

// DBSize returns the total number of keys across all data stores.
func (s *Store) DBSize() int {
	return s.Strings.Len() + s.Hashes.Len() + s.Lists.Len() + s.Sets.Len() + s.ZSets.Len()
}

// FlushDB removes all keys from all data stores and clears all TTL entries.
func (s *Store) FlushDB() {
	s.Strings.Flush()
	s.Hashes.Flush()
	s.Lists.Flush()
	s.Sets.Flush()
	s.ZSets.Flush()
	s.TTL.Flush()
	s.metaMu.Lock()
	s.meta = make(map[string]*KeyMetadata)
	s.metaMu.Unlock()
}

// RenameKey renames a key in whichever store holds it.
// Returns false if the source key does not exist.
func (s *Store) RenameKey(oldKey, newKey string) bool {
	// Transfer metadata
	s.metaMu.Lock()
	if m, ok := s.meta[oldKey]; ok {
		s.meta[newKey] = m
		delete(s.meta, oldKey)
	}
	s.metaMu.Unlock()

	// String store
	if val, ok := s.Strings.GetRaw(oldKey); ok {
		s.Strings.Delete(oldKey)
		s.Strings.SetRaw(newKey, val)
		// Transfer TTL
		if ttl, ok := s.TTL.GetDeadline(oldKey); ok {
			s.TTL.Remove(oldKey)
			s.TTL.SetDeadline(newKey, ttl)
		}
		return true
	}
	// Hash store
	if val, ok := s.Hashes.GetRaw(oldKey); ok {
		s.Hashes.Delete(oldKey)
		s.Hashes.SetRaw(newKey, val)
		if ttl, ok := s.TTL.GetDeadline(oldKey); ok {
			s.TTL.Remove(oldKey)
			s.TTL.SetDeadline(newKey, ttl)
		}
		return true
	}
	// List store
	if val, ok := s.Lists.GetRaw(oldKey); ok {
		s.Lists.Delete(oldKey)
		s.Lists.SetRaw(newKey, val)
		if ttl, ok := s.TTL.GetDeadline(oldKey); ok {
			s.TTL.Remove(oldKey)
			s.TTL.SetDeadline(newKey, ttl)
		}
		return true
	}
	// Set store
	if val, ok := s.Sets.GetRaw(oldKey); ok {
		s.Sets.Delete(oldKey)
		s.Sets.SetRaw(newKey, val)
		if ttl, ok := s.TTL.GetDeadline(oldKey); ok {
			s.TTL.Remove(oldKey)
			s.TTL.SetDeadline(newKey, ttl)
		}
		return true
	}
	// ZSet store
	if val, ok := s.ZSets.GetRaw(oldKey); ok {
		s.ZSets.Delete(oldKey)
		s.ZSets.SetRaw(newKey, val)
		if ttl, ok := s.TTL.GetDeadline(oldKey); ok {
			s.TTL.Remove(oldKey)
			s.TTL.SetDeadline(newKey, ttl)
		}
		return true
	}
	return false
}

// Evict evicts count keys according to the specified policy.
func (s *Store) Evict(policy string, count int) int {
	evicted := 0
	if count <= 0 {
		return 0
	}

	keys := s.AllKeys()
	if len(keys) == 0 {
		return 0
	}

	for i := 0; i < count; i++ {
		var victim string
		switch policy {
		case "allkeys-random":
			victim = keys[rand.Intn(len(keys))]
		case "volatile-random":
			var volKeys []string
			for _, k := range keys {
				if s.TTL.Exists(k) {
					volKeys = append(volKeys, k)
				}
			}
			if len(volKeys) > 0 {
				victim = volKeys[rand.Intn(len(volKeys))]
			}
		case "allkeys-lru":
			victim = s.selectLRUVictim(keys)
		case "volatile-lru":
			var volKeys []string
			for _, k := range keys {
				if s.TTL.Exists(k) {
					volKeys = append(volKeys, k)
				}
			}
			if len(volKeys) > 0 {
				victim = s.selectLRUVictim(volKeys)
			}
		case "allkeys-lfu":
			victim = s.selectLFUVictim(keys)
		case "volatile-lfu":
			var volKeys []string
			for _, k := range keys {
				if s.TTL.Exists(k) {
					volKeys = append(volKeys, k)
				}
			}
			if len(volKeys) > 0 {
				victim = s.selectLFUVictim(volKeys)
			}
		}

		if victim != "" {
			if s.DeleteKey(victim) {
				evicted++
				// Refresh keys list
				keys = s.AllKeys()
				if len(keys) == 0 {
					break
				}
			}
		} else {
			break // No victim found (e.g. volatile policy but no keys with TTL)
		}
	}

	if evicted > 0 {
		runtime.GC()
	}
	return evicted
}

func (s *Store) selectLRUVictim(candidates []string) string {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	var oldestKey string
	var oldestTime time.Time

	// Approximated LRU by sampling up to 5 random keys
	sampleSize := 5
	if len(candidates) < sampleSize {
		sampleSize = len(candidates)
	}

	indices := rand.Perm(len(candidates))[:sampleSize]
	for idx, valIdx := range indices {
		k := candidates[valIdx]
		m, ok := s.meta[k]
		if idx == 0 {
			oldestKey = k
			if ok {
				oldestTime = m.LastAccess
			} else {
				oldestTime = time.Time{}
			}
			continue
		}
		var accessTime time.Time
		if ok {
			accessTime = m.LastAccess
		}
		if accessTime.Before(oldestTime) {
			oldestTime = accessTime
			oldestKey = k
		}
	}
	return oldestKey
}

func (s *Store) selectLFUVictim(candidates []string) string {
	s.metaMu.Lock()
	defer s.metaMu.Unlock()

	var leastKey string
	var leastCount int64

	sampleSize := 5
	if len(candidates) < sampleSize {
		sampleSize = len(candidates)
	}

	indices := rand.Perm(len(candidates))[:sampleSize]
	for idx, valIdx := range indices {
		k := candidates[valIdx]
		m, ok := s.meta[k]
		if idx == 0 {
			leastKey = k
			if ok {
				leastCount = m.AccessCount
			} else {
				leastCount = 0
			}
			continue
		}
		var count int64
		if ok {
			count = m.AccessCount
		}
		if count < leastCount {
			leastCount = count
			leastKey = k
		}
	}
	return leastKey
}
