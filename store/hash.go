package store

import (
	"sync"
)

// HashStore manages hash (field-value map) data with its own read-write mutex.
// Each top-level key maps to a map of fields to values.
type HashStore struct {
	mu   sync.RWMutex
	data map[string]map[string]string
}

// NewHashStore creates a new empty HashStore.
func NewHashStore() *HashStore {
	return &HashStore{
		data: make(map[string]map[string]string),
	}
}

// HSet sets one or more field-value pairs in the hash stored at key.
// Returns the number of new fields added (fields that were updated are not counted).
func (s *HashStore) HSet(key string, fieldValues map[string]string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}
	added := 0
	for field, value := range fieldValues {
		if _, exists := s.data[key][field]; !exists {
			added++
		}
		s.data[key][field] = value
	}
	return added
}

// HGet retrieves the value of a field in the hash stored at key.
func (s *HashStore) HGet(key, field string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return "", false
	}
	val, ok := h[field]
	return val, ok
}

// HGetAll returns all field-value pairs in the hash stored at key.
// Returns nil if the key does not exist.
func (s *HashStore) HGetAll(key string) map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return nil
	}
	// Return a copy to avoid data races on the caller side
	result := make(map[string]string, len(h))
	for k, v := range h {
		result[k] = v
	}
	return result
}

// HDel deletes one or more fields from the hash stored at key.
// Returns the number of fields actually removed.
func (s *HashStore) HDel(key string, fields []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	h, ok := s.data[key]
	if !ok {
		return 0
	}
	removed := 0
	for _, field := range fields {
		if _, exists := h[field]; exists {
			delete(h, field)
			removed++
		}
	}
	// Auto-delete empty hashes
	if len(h) == 0 {
		delete(s.data, key)
	}
	return removed
}

// HLen returns the number of fields in the hash stored at key.
func (s *HashStore) HLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data[key])
}

// HKeys returns all field names in the hash stored at key.
func (s *HashStore) HKeys(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return []string{}
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	return keys
}

// HExists checks whether a field exists in the hash stored at key.
func (s *HashStore) HExists(key, field string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return false
	}
	_, exists := h[field]
	return exists
}

// HMGet retrieves the values for multiple fields in the hash stored at key.
// For non-existent fields, nil is returned in the corresponding position.
func (s *HashStore) HMGet(key string, fields []string) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.data[key]
	result := make([]interface{}, len(fields))
	for i, field := range fields {
		if h != nil {
			if val, ok := h[field]; ok {
				result[i] = val
			}
		}
	}
	return result
}

// Exists checks whether a key exists in the hash store.
func (s *HashStore) Exists(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

// Delete removes a hash by its key. Returns true if the key existed.
func (s *HashStore) Delete(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

// GetRaw returns the raw hash map for internal use (e.g., RENAME).
func (s *HashStore) GetRaw(key string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.data[key]
	if !ok {
		return nil, false
	}
	// Return a copy
	cp := make(map[string]string, len(h))
	for k, v := range h {
		cp[k] = v
	}
	return cp, true
}

// SetRaw sets a raw hash map for internal use (e.g., RENAME).
func (s *HashStore) SetRaw(key string, val map[string]string) {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// Keys returns all keys in the hash store.
func (s *HashStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys in the hash store.
func (s *HashStore) Len() int {
	s.mu.RLock()
	l := len(s.data)
	s.mu.RUnlock()
	return l
}

// Flush removes all keys from the hash store.
func (s *HashStore) Flush() {
	s.mu.Lock()
	s.data = make(map[string]map[string]string)
	s.mu.Unlock()
}
