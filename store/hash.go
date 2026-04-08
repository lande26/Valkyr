package store

import (
	"fmt"
	"strconv"
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

// HIncrBy increments the integer value of a hash field by the given increment.
// If the key or field does not exist, it is set to 0 before executing the operation.
func (s *HashStore) HIncrBy(key, field string, increment int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}

	valStr, ok := s.data[key][field]
	var val int64
	if ok {
		var err error
		val, err = strconv.ParseInt(valStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not an integer")
		}
	}

	val += increment
	s.data[key][field] = strconv.FormatInt(val, 10)
	return val, nil
}

// HIncrByFloat increments the float value of a hash field by the given increment.
// If the key or field does not exist, it is set to 0 before executing the operation.
func (s *HashStore) HIncrByFloat(key, field string, increment float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		s.data[key] = make(map[string]string)
	}

	valStr, ok := s.data[key][field]
	var val float64
	if ok {
		var err error
		val, err = strconv.ParseFloat(valStr, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR hash value is not a valid float")
		}
	}

	val += increment
	s.data[key][field] = fmt.Sprintf("%g", val)
	return val, nil
}

// HVals returns all values in the hash stored at key.
func (s *HashStore) HVals(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.data[key]
	if !ok {
		return []string{}
	}
	vals := make([]string, 0, len(h))
	for _, v := range h {
		vals = append(vals, v)
	}
	return vals
}

// HStrLen returns the string length of the value associated with field in the hash stored at key.
// If the key or field does not exist, 0 is returned.
func (s *HashStore) HStrLen(key, field string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	h, ok := s.data[key]
	if !ok {
		return 0
	}
	val, ok := h[field]
	if !ok {
		return 0
	}
	return len(val)
}
