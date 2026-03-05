package store

import (
	"strconv"
	"sync"
)

// StringStore manages string key-value pairs with its own read-write mutex
// for concurrent access. It is the backing store for Redis string commands.
type StringStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStringStore creates a new empty StringStore.
func NewStringStore() *StringStore {
	return &StringStore{
		data: make(map[string]string),
	}
}

// Set stores a string value for the given key, overwriting any previous value.
func (s *StringStore) Set(key, value string) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

// SetNX sets the key only if it does not already exist. Returns true if set.
func (s *StringStore) SetNX(key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; ok {
		return false
	}
	s.data[key] = value
	return true
}

// SetXX sets the key only if it already exists. Returns true if set.
func (s *StringStore) SetXX(key, value string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	s.data[key] = value
	return true
}

// Get retrieves the value for the given key. Returns the value and true if found.
func (s *StringStore) Get(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

// GetRaw returns the raw string value for internal use (e.g., RENAME).
func (s *StringStore) GetRaw(key string) (string, bool) {
	s.mu.RLock()
	val, ok := s.data[key]
	s.mu.RUnlock()
	return val, ok
}

// SetRaw sets a raw string value for internal use (e.g., RENAME).
func (s *StringStore) SetRaw(key, value string) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

// Delete removes a key from the string store. Returns true if the key existed.
func (s *StringStore) Delete(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

// Exists checks whether a key exists in the string store.
func (s *StringStore) Exists(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

// MSet sets multiple key-value pairs atomically.
func (s *StringStore) MSet(pairs map[string]string) {
	s.mu.Lock()
	for k, v := range pairs {
		s.data[k] = v
	}
	s.mu.Unlock()
}

// MGet retrieves values for multiple keys. Returns values in the same order as keys;
// nil entries indicate non-existent keys.
func (s *StringStore) MGet(keys []string) []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]interface{}, len(keys))
	for i, key := range keys {
		if val, ok := s.data[key]; ok {
			result[i] = val
		} else {
			result[i] = nil
		}
	}
	return result
}

// IncrBy increments the integer value stored at key by delta.
// If the key does not exist, it is set to 0 before incrementing.
// Returns the new value and an error if the stored value is not an integer.
func (s *StringStore) IncrBy(key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val, ok := s.data[key]
	var current int64
	if ok {
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0, errNotInteger
		}
		current = n
	}
	current += delta
	s.data[key] = strconv.FormatInt(current, 10)
	return current, nil
}

// Append appends the value to the existing string at key.
// If the key does not exist, it is created with the given value.
// Returns the length of the new string.
func (s *StringStore) Append(key, value string) int {
	s.mu.Lock()
	s.data[key] += value
	l := len(s.data[key])
	s.mu.Unlock()
	return l
}

// StrLen returns the length of the string stored at key.
// Returns 0 if the key does not exist.
func (s *StringStore) StrLen(key string) int {
	s.mu.RLock()
	l := len(s.data[key])
	s.mu.RUnlock()
	return l
}

// Keys returns all keys in the string store.
func (s *StringStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys in the string store.
func (s *StringStore) Len() int {
	s.mu.RLock()
	l := len(s.data)
	s.mu.RUnlock()
	return l
}

// Flush removes all keys from the string store.
func (s *StringStore) Flush() {
	s.mu.Lock()
	s.data = make(map[string]string)
	s.mu.Unlock()
}

// errNotInteger is a sentinel used to indicate a string cannot be parsed as int64.
var errNotInteger = &integerError{}

type integerError struct{}

func (e *integerError) Error() string {
	return "ERR value is not an integer or out of range"
}
