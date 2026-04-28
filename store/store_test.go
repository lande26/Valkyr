package store

import (
	"strconv"
	"testing"
	"time"
)

func TestStringStore(t *testing.T) {
	s := NewStringStore()

	s.Set("key1", "val1")
	val, ok := s.Get("key1")
	if !ok || val != "val1" {
		t.Errorf("expected val1, got %s", val)
	}

	if s.SetNX("key1", "newval") {
		t.Errorf("SetNX should have failed for existing key")
	}

	if !s.SetNX("key2", "val2") {
		t.Errorf("SetNX should have succeeded for new key")
	}

	if !s.SetXX("key1", "updated") {
		t.Errorf("SetXX should have succeeded for existing key")
	}

	s.IncrBy("num", 10)
	numVal, _ := s.Get("num")
	if numVal != "10" {
		t.Errorf("expected 10, got %s", numVal)
	}

	s.IncrBy("num", -2)
	numVal, _ = s.Get("num")
	if numVal != "8" {
		t.Errorf("expected 8, got %s", numVal)
	}

	s.Append("key1", "append")
	val, _ = s.Get("key1")
	if val != "updatedappend" {
		t.Errorf("expected updatedappend, got %s", val)
	}
}

func TestHashStore(t *testing.T) {
	s := NewHashStore()

	s.HSet("h1", map[string]string{"f1": "v1", "f2": "v2"})
	v, ok := s.HGet("h1", "f1")
	if !ok || v != "v1" {
		t.Errorf("expected v1, got %s", v)
	}

	inc, _ := s.HIncrBy("h1", "intf", 5)
	if inc != 5 {
		t.Errorf("expected 5, got %d", inc)
	}

	incFloat, _ := s.HIncrByFloat("h1", "floatf", 2.5)
	if incFloat != 2.5 {
		t.Errorf("expected 2.5, got %f", incFloat)
	}

	if s.HLen("h1") != 4 {
		t.Errorf("expected length 4, got %d", s.HLen("h1"))
	}

	s.HDel("h1", []string{"f1"})
	if s.HExists("h1", "f1") {
		t.Errorf("f1 should be deleted")
	}
}

func TestListStore(t *testing.T) {
	s := NewListStore()

	s.RPush("list", []string{"a", "b", "c"})
	s.LPush("list", []string{"x"})

	// list is: ["x", "a", "b", "c"]
	r := s.LRange("list", 0, -1)
	expected := []string{"x", "a", "b", "c"}
	for i, v := range r {
		if v != expected[i] {
			t.Errorf("at %d, expected %s, got %s", i, expected[i], v)
		}
	}

	s.LInsert("list", "BEFORE", "b", "inserted")
	// list is: ["x", "a", "inserted", "b", "c"]
	r = s.LRange("list", 0, -1)
	if r[2] != "inserted" {
		t.Errorf("expected inserted at idx 2, got %s", r[2])
	}

	s.LTrim("list", 1, 3)
	// list is: ["a", "inserted", "b"]
	if s.LLen("list") != 3 {
		t.Errorf("expected length 3, got %d", s.LLen("list"))
	}

	s.LRem("list", 0, "inserted")
	// list is: ["a", "b"]
	r = s.LRange("list", 0, -1)
	if len(r) != 2 || r[1] != "b" {
		t.Errorf("expected list [a, b], got %v", r)
	}
}

func TestSetStore(t *testing.T) {
	s := NewSetStore()

	s.SAdd("s1", []string{"a", "b", "c"})
	s.SAdd("s2", []string{"b", "c", "d"})

	inter := s.SInter([]string{"s1", "s2"})
	if len(inter) != 2 {
		t.Errorf("expected 2 elements in intersection, got %d", len(inter))
	}

	union := s.SUnion([]string{"s1", "s2"})
	if len(union) != 4 {
		t.Errorf("expected 4 elements in union, got %d", len(union))
	}

	diff := s.SDiff([]string{"s1", "s2"})
	if len(diff) != 1 || diff[0] != "a" {
		t.Errorf("expected diff to be [a], got %v", diff)
	}

	popped := s.SPop("s1", 1)
	if len(popped) != 1 {
		t.Errorf("expected 1 popped member")
	}

	if s.SCard("s1") != 2 {
		t.Errorf("expected s1 card 2, got %d", s.SCard("s1"))
	}
}

func TestZSetStore(t *testing.T) {
	s := NewZSetStore()

	s.ZAdd("z1", 10.5, "memA")
	s.ZAdd("z1", 5.0, "memB")
	s.ZAdd("z1", 20.0, "memC")

	// Order should be memB (5), memA (10.5), memC (20)
	r := s.ZRange("z1", 0, -1)
	if len(r) != 3 || r[0].Member != "memB" || r[1].Member != "memA" || r[2].Member != "memC" {
		t.Errorf("expected order memB, memA, memC; got %+v", r)
	}

	rank, _ := s.ZRank("z1", "memA")
	if rank != 1 {
		t.Errorf("expected rank 1, got %d", rank)
	}

	revRank, _ := s.ZRevRank("z1", "memA")
	if revRank != 1 {
		t.Errorf("expected revRank 1, got %d", revRank)
	}

	cnt := s.ZCount("z1", 5.0, 15.0)
	if cnt != 2 {
		t.Errorf("expected count 2 in range [5, 15], got %d", cnt)
	}

	s.ZRem("z1", []string{"memA"})
	if s.ZCard("z1") != 2 {
		t.Errorf("expected card 2 after remove, got %d", s.ZCard("z1"))
	}
}

func TestStoreEviction(t *testing.T) {
	st := NewStore()

	// Populate metadata
	for i := 0; i < 10; i++ {
		key := "k" + strconv.Itoa(i)
		st.Strings.Set(key, "v")
		st.Touch(key)
	}

	// Wait a tiny bit and access k0 to update its LRU/LFU stats
	time.Sleep(2 * time.Millisecond)
	st.Touch("k0")
	st.Touch("k0") // increment LFU count for k0

	// Test random eviction
	st.Evict("allkeys-random", 2)
	if st.DBSize() != 8 {
		t.Errorf("expected size 8, got %d", st.DBSize())
	}
}
