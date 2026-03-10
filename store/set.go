package store

import (
	"sync"
)

// SetStore manages set (unordered collection of unique strings) data
// with its own read-write mutex.
type SetStore struct {
	mu   sync.RWMutex
	data map[string]map[string]struct{}
}

// NewSetStore creates a new empty SetStore.
func NewSetStore() *SetStore {
	return &SetStore{
		data: make(map[string]map[string]struct{}),
	}
}

// SAdd adds one or more members to the set stored at key.
// Returns the number of members actually added (not already present).
func (s *SetStore) SAdd(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]struct{})
	}
	added := 0
	for _, member := range members {
		if _, exists := s.data[key][member]; !exists {
			s.data[key][member] = struct{}{}
			added++
		}
	}
	return added
}

// SRem removes one or more members from the set stored at key.
// Returns the number of members actually removed.
func (s *SetStore) SRem(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	set, ok := s.data[key]
	if !ok {
		return 0
	}
	removed := 0
	for _, member := range members {
		if _, exists := set[member]; exists {
			delete(set, member)
			removed++
		}
	}
	// Auto-delete empty sets
	if len(set) == 0 {
		delete(s.data, key)
	}
	return removed
}

// SMembers returns all members of the set stored at key.
func (s *SetStore) SMembers(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set, ok := s.data[key]
	if !ok {
		return []string{}
	}
	members := make([]string, 0, len(set))
	for member := range set {
		members = append(members, member)
	}
	return members
}

// SIsMember checks whether member is in the set stored at key.
func (s *SetStore) SIsMember(key, member string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set, ok := s.data[key]
	if !ok {
		return false
	}
	_, exists := set[member]
	return exists
}

// SCard returns the cardinality (number of elements) of the set stored at key.
func (s *SetStore) SCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data[key])
}

// SInter returns the intersection of all specified sets.
// If any key does not exist, the result is an empty set.
func (s *SetStore) SInter(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(keys) == 0 {
		return []string{}
	}

	// Start with the first set
	first, ok := s.data[keys[0]]
	if !ok {
		return []string{}
	}

	result := make([]string, 0)
	for member := range first {
		inAll := true
		for _, key := range keys[1:] {
			set, ok := s.data[key]
			if !ok {
				return []string{}
			}
			if _, exists := set[member]; !exists {
				inAll = false
				break
			}
		}
		if inAll {
			result = append(result, member)
		}
	}
	return result
}

// SUnion returns the union of all specified sets.
func (s *SetStore) SUnion(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	merged := make(map[string]struct{})
	for _, key := range keys {
		set, ok := s.data[key]
		if !ok {
			continue
		}
		for member := range set {
			merged[member] = struct{}{}
		}
	}
	result := make([]string, 0, len(merged))
	for member := range merged {
		result = append(result, member)
	}
	return result
}

// SDiff returns the members of the first set that are not in any of the subsequent sets.
func (s *SetStore) SDiff(keys []string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(keys) == 0 {
		return []string{}
	}

	first, ok := s.data[keys[0]]
	if !ok {
		return []string{}
	}

	result := make([]string, 0)
	for member := range first {
		inOther := false
		for _, key := range keys[1:] {
			set, ok := s.data[key]
			if ok {
				if _, exists := set[member]; exists {
					inOther = true
					break
				}
			}
		}
		if !inOther {
			result = append(result, member)
		}
	}
	return result
}

// Exists checks whether a key exists in the set store.
func (s *SetStore) Exists(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

// Delete removes a set by its key. Returns true if the key existed.
func (s *SetStore) Delete(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

// GetRaw returns the raw set for internal use (e.g., RENAME).
func (s *SetStore) GetRaw(key string) (map[string]struct{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	set, ok := s.data[key]
	if !ok {
		return nil, false
	}
	cp := make(map[string]struct{}, len(set))
	for k := range set {
		cp[k] = struct{}{}
	}
	return cp, true
}

// SetRaw sets a raw set for internal use (e.g., RENAME).
func (s *SetStore) SetRaw(key string, val map[string]struct{}) {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// Keys returns all keys in the set store.
func (s *SetStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys in the set store.
func (s *SetStore) Len() int {
	s.mu.RLock()
	l := len(s.data)
	s.mu.RUnlock()
	return l
}

// Flush removes all keys from the set store.
func (s *SetStore) Flush() {
	s.mu.Lock()
	s.data = make(map[string]map[string]struct{})
	s.mu.Unlock()
}
