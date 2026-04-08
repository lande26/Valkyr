package store

import (
	"errors"
	"strings"
	"sync"
)

// ListStore manages list (ordered slice of strings) data with its own read-write mutex.
type ListStore struct {
	mu   sync.RWMutex
	data map[string][]string
}

// NewListStore creates a new empty ListStore.
func NewListStore() *ListStore {
	return &ListStore{
		data: make(map[string][]string),
	}
}

// ErrIndexOutOfRange is returned when a list index is out of bounds.
var ErrIndexOutOfRange = errors.New("ERR index out of range")

// LPush inserts one or more values at the head (left) of the list stored at key.
// Values are inserted one at a time from left to right, so the last value in the
// arguments list will be at the head after the operation. Returns the new list length.
func (s *ListStore) LPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.data[key]
	for i := len(values) - 1; i >= 0; i-- {
		list = append([]string{values[i]}, list...)
	}
	s.data[key] = list
	return len(list)
}

// RPush appends one or more values at the tail (right) of the list stored at key.
// Returns the new list length.
func (s *ListStore) RPush(key string, values []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append(s.data[key], values...)
	return len(s.data[key])
}

// LPop removes and returns the first element of the list stored at key.
// Returns the element and true, or empty string and false if the list is empty or absent.
func (s *ListStore) LPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return "", false
	}
	val := list[0]
	s.data[key] = list[1:]
	if len(s.data[key]) == 0 {
		delete(s.data, key)
	}
	return val, true
}

// RPop removes and returns the last element of the list stored at key.
// Returns the element and true, or empty string and false if the list is empty or absent.
func (s *ListStore) RPop(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return "", false
	}
	val := list[len(list)-1]
	s.data[key] = list[:len(list)-1]
	if len(s.data[key]) == 0 {
		delete(s.data, key)
	}
	return val, true
}

// LLen returns the length of the list stored at key.
func (s *ListStore) LLen(key string) int {
	s.mu.RLock()
	l := len(s.data[key])
	s.mu.RUnlock()
	return l
}

// LRange returns a slice of elements from the list stored at key, from start to stop (inclusive).
// Negative indices are supported: -1 is the last element, -2 the second to last, etc.
func (s *ListStore) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.data[key]
	if !ok {
		return []string{}
	}
	length := len(list)

	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result
}

// LIndex returns the element at the given index in the list stored at key.
// Negative indices are supported. Returns the element and nil on success,
// or empty string and ErrIndexOutOfRange if the index is out of bounds.
func (s *ListStore) LIndex(key string, index int) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.data[key]
	if !ok {
		return "", ErrIndexOutOfRange
	}
	length := len(list)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return "", ErrIndexOutOfRange
	}
	return list[index], nil
}

// LSet sets the list element at the given index to the given value.
// Negative indices are supported. Returns ErrIndexOutOfRange if the index is out of bounds,
// or if the key does not exist.
func (s *ListStore) LSet(key string, index int, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, ok := s.data[key]
	if !ok {
		return ErrIndexOutOfRange
	}
	length := len(list)
	if index < 0 {
		index = length + index
	}
	if index < 0 || index >= length {
		return ErrIndexOutOfRange
	}
	list[index] = value
	return nil
}

// Exists checks whether a key exists in the list store.
func (s *ListStore) Exists(key string) bool {
	s.mu.RLock()
	_, ok := s.data[key]
	s.mu.RUnlock()
	return ok
}

// Delete removes a list by its key. Returns true if the key existed.
func (s *ListStore) Delete(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.mu.Unlock()
	return ok
}

// GetRaw returns the raw list for internal use (e.g., RENAME).
func (s *ListStore) GetRaw(key string) ([]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	list, ok := s.data[key]
	if !ok {
		return nil, false
	}
	cp := make([]string, len(list))
	copy(cp, list)
	return cp, true
}

// SetRaw sets a raw list for internal use (e.g., RENAME).
func (s *ListStore) SetRaw(key string, val []string) {
	s.mu.Lock()
	s.data[key] = val
	s.mu.Unlock()
}

// Keys returns all keys in the list store.
func (s *ListStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Len returns the number of keys in the list store.
func (s *ListStore) Len() int {
	s.mu.RLock()
	l := len(s.data)
	s.mu.RUnlock()
	return l
}

// Flush removes all keys from the list store.
func (s *ListStore) Flush() {
	s.mu.Lock()
	s.data = make(map[string][]string)
	s.mu.Unlock()
}

// LRem removes elements equal to value from the list stored at key.
// If count > 0: removes elements equal to value moving from head to tail.
// If count < 0: removes elements equal to value moving from tail to head.
// If count == 0: removes all elements equal to value.
// Returns the number of removed elements.
func (s *ListStore) LRem(key string, count int, value string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return 0
	}

	removed := 0
	var newList []string

	if count == 0 {
		for _, v := range list {
			if v == value {
				removed++
			} else {
				newList = append(newList, v)
			}
		}
	} else if count > 0 {
		for _, v := range list {
			if v == value && removed < count {
				removed++
			} else {
				newList = append(newList, v)
			}
		}
	} else { // count < 0
		absLimit := -count
		for i := len(list) - 1; i >= 0; i-- {
			v := list[i]
			if v == value && removed < absLimit {
				removed++
			} else {
				newList = append([]string{v}, newList...)
			}
		}
	}

	if removed > 0 {
		s.data[key] = newList
		if len(newList) == 0 {
			delete(s.data, key)
		}
	}
	return removed
}

// LTrim trims the list at key to the specified start/stop range (inclusive).
func (s *ListStore) LTrim(key string, start, stop int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return
	}

	length := len(list)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		delete(s.data, key)
		return
	}

	s.data[key] = list[start : stop+1]
}

// LInsert inserts value before or after pivot in the list stored at key.
// position must be "BEFORE" or "AFTER" (case-insensitive).
// Returns the new list length, or -1 if the pivot was not found.
func (s *ListStore) LInsert(key string, position string, pivot string, value string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.data[key]
	if !ok || len(list) == 0 {
		return 0, nil
	}

	pos := strings.ToUpper(position)
	if pos != "BEFORE" && pos != "AFTER" {
		return 0, errors.New("ERR syntax error")
	}

	pivotIdx := -1
	for i, v := range list {
		if v == pivot {
			pivotIdx = i
			break
		}
	}

	if pivotIdx == -1 {
		return -1, nil
	}

	var newList []string
	if pos == "BEFORE" {
		newList = append(newList, list[:pivotIdx]...)
		newList = append(newList, value)
		newList = append(newList, list[pivotIdx:]...)
	} else {
		newList = append(newList, list[:pivotIdx+1]...)
		newList = append(newList, value)
		newList = append(newList, list[pivotIdx+1:]...)
	}

	s.data[key] = newList
	return len(newList), nil
}
