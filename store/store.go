// Package store implements the in-memory data stores for Valkyr.
// It provides a unified Store aggregate that holds all data type stores
// (strings, hashes, lists, sets) and TTL management.
package store

// Store is the top-level aggregate that holds all individual data stores.
// It is passed via constructor injection to the server and router — no global state.
type Store struct {
	Strings *StringStore
	Hashes  *HashStore
	Lists   *ListStore
	Sets    *SetStore
	TTL     *TTLStore
}

// NewStore creates a new Store with all sub-stores initialized.
// The deleteFunc on the TTL store is wired to remove keys from all data stores.
func NewStore() *Store {
	s := &Store{
		Strings: NewStringStore(),
		Hashes:  NewHashStore(),
		Lists:   NewListStore(),
		Sets:    NewSetStore(),
	}
	s.TTL = NewTTLStore(func(key string) {
		s.Strings.Delete(key)
		s.Hashes.Delete(key)
		s.Lists.Delete(key)
		s.Sets.Delete(key)
	})
	return s
}

// KeyExists checks whether a key exists in any of the data stores.
func (s *Store) KeyExists(key string) bool {
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
	return false
}

// KeyType returns the Redis type name of the key, or "none" if the key doesn't exist.
func (s *Store) KeyType(key string) string {
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
	return "none"
}

// DeleteKey removes a key from whichever store holds it. Returns true if deleted.
func (s *Store) DeleteKey(key string) bool {
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
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	return keys
}

// DBSize returns the total number of keys across all data stores.
func (s *Store) DBSize() int {
	return s.Strings.Len() + s.Hashes.Len() + s.Lists.Len() + s.Sets.Len()
}

// FlushDB removes all keys from all data stores and clears all TTL entries.
func (s *Store) FlushDB() {
	s.Strings.Flush()
	s.Hashes.Flush()
	s.Lists.Flush()
	s.Sets.Flush()
	s.TTL.Flush()
}

// RenameKey renames a key in whichever store holds it.
// Returns false if the source key does not exist.
func (s *Store) RenameKey(oldKey, newKey string) bool {
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
	return false
}
