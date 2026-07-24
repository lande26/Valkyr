package store

import (
	"math/rand"
	"sync"
	"time"
)

// Skip list configuration parameters modeled after Redis sorted sets (t_zset.c).
// Max level 32 supports up to 2^64 elements efficiently.
const zsetMaxLevel = 32
const zsetP = 0.25

type zskiplistNode struct {
	ele      string
	score    float64
	backward *zskiplistNode
	level    []struct {
		forward *zskiplistNode
		span    int
	}
}

func newZskiplistNode(level int, score float64, ele string) *zskiplistNode {
	return &zskiplistNode{
		ele:   ele,
		score: score,
		level: make([]struct {
			forward *zskiplistNode
			span    int
		}, level),
	}
}

type zskiplist struct {
	header, tail *zskiplistNode
	length       int
	level        int
	randSource   rand.Source
}

func newZskiplist() *zskiplist {
	zsl := &zskiplist{
		level:      1,
		length:     0,
		randSource: rand.NewSource(time.Now().UnixNano()),
		header:     newZskiplistNode(zsetMaxLevel, 0, ""),
	}
	for i := 0; i < zsetMaxLevel; i++ {
		zsl.header.level[i].forward = nil
		zsl.header.level[i].span = 0
	}
	zsl.header.backward = nil
	zsl.tail = nil
	return zsl
}

func (zsl *zskiplist) randomLevel() int {
	r := rand.New(zsl.randSource)
	level := 1
	for r.Float64() < zsetP && level < zsetMaxLevel {
		level++
	}
	return level
}

func (zsl *zskiplist) insert(score float64, ele string) *zskiplistNode {
	update := make([]*zskiplistNode, zsetMaxLevel)
	rank := make([]int, zsetMaxLevel)
	x := zsl.header

	for i := zsl.level - 1; i >= 0; i-- {
		if i == zsl.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele < ele)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}

	level := zsl.randomLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			rank[i] = 0
			update[i] = zsl.header
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}

	x = newZskiplistNode(level, score, ele)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}

	if update[0] == zsl.header {
		x.backward = nil
	} else {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

func (zsl *zskiplist) deleteNode(x *zskiplistNode, update []*zskiplistNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.header.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

func (zsl *zskiplist) delete(score float64, ele string) bool {
	update := make([]*zskiplistNode, zsetMaxLevel)
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele < ele)) {
			x = x.level[i].forward
		}
		update[i] = x
	}
	x = x.level[0].forward
	if x != nil && score == x.score && ele == x.ele {
		zsl.deleteNode(x, update)
		return true
	}
	return false
}

func (zsl *zskiplist) getRank(score float64, ele string) int {
	rank := 0
	x := zsl.header
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.score < score ||
				(x.level[i].forward.score == score && x.level[i].forward.ele <= ele)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}
		if x != zsl.header && x.ele == ele {
			return rank
		}
	}
	return 0
}

func (zsl *zskiplist) getNodeByRank(rank int) *zskiplistNode {
	if rank <= 0 || rank > zsl.length {
		return nil
	}
	x := zsl.header
	accum := 0
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && accum+x.level[i].span <= rank {
			accum += x.level[i].span
			x = x.level[i].forward
		}
		if accum == rank {
			return x
		}
	}
	return nil
}

type ZSetElement struct {
	Member string
	Score  float64
}

type SortedSet struct {
	dict map[string]float64
	zsl  *zskiplist
}

func NewSortedSet() *SortedSet {
	return &SortedSet{
		dict: make(map[string]float64),
		zsl:  newZskiplist(),
	}
}

type ZSetStore struct {
	mu   sync.RWMutex
	data map[string]*SortedSet
}

func NewZSetStore() *ZSetStore {
	return &ZSetStore{
		data: make(map[string]*SortedSet),
	}
}

func (s *ZSetStore) Exists(key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.data[key]
	return ok
}

func (s *ZSetStore) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return ok
}

func (s *ZSetStore) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

func (s *ZSetStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *ZSetStore) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]*SortedSet)
}

func (s *ZSetStore) GetRaw(key string) (*SortedSet, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *ZSetStore) SetRaw(key string, val *SortedSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = val
}

func (s *ZSetStore) ZAdd(key string, score float64, member string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	zs, ok := s.data[key]
	if !ok {
		zs = NewSortedSet()
		s.data[key] = zs
	}

	oldScore, exists := zs.dict[member]
	if exists {
		if oldScore == score {
			return false
		}
		zs.zsl.delete(oldScore, member)
	}

	zs.dict[member] = score
	zs.zsl.insert(score, member)
	return !exists
}

func (s *ZSetStore) ZRem(key string, members []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	zs, ok := s.data[key]
	if !ok {
		return 0
	}

	removed := 0
	for _, m := range members {
		if score, ok := zs.dict[m]; ok {
			zs.zsl.delete(score, m)
			delete(zs.dict, m)
			removed++
		}
	}

	if len(zs.dict) == 0 {
		delete(s.data, key)
	}
	return removed
}

func (s *ZSetStore) ZCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return 0
	}
	return zs.zsl.length
}

func (s *ZSetStore) ZScore(key string, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return 0, false
	}
	score, exists := zs.dict[member]
	return score, exists
}

func (s *ZSetStore) ZRank(key string, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return 0, false
	}
	score, exists := zs.dict[member]
	if !exists {
		return 0, false
	}
	rank := zs.zsl.getRank(score, member)
	if rank == 0 {
		return 0, false
	}
	return rank - 1, true
}

func (s *ZSetStore) ZRevRank(key string, member string) (int, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return 0, false
	}
	score, exists := zs.dict[member]
	if !exists {
		return 0, false
	}
	rank := zs.zsl.getRank(score, member)
	if rank == 0 {
		return 0, false
	}
	return zs.zsl.length - rank, true
}

func (s *ZSetStore) ZRange(key string, start, stop int) []ZSetElement {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return nil
	}

	length := zs.zsl.length
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}

	if start > stop || start >= length {
		return nil
	}
	if stop >= length {
		stop = length - 1
	}

	node := zs.zsl.getNodeByRank(start + 1)
	if node == nil {
		return nil
	}

	result := make([]ZSetElement, 0, stop-start+1)
	for i := start; i <= stop && node != nil; i++ {
		result = append(result, ZSetElement{Member: node.ele, Score: node.score})
		if len(node.level) > 0 {
			node = node.level[0].forward
		} else {
			break
		}
	}
	return result
}

func (s *ZSetStore) ZRevRange(key string, start, stop int) []ZSetElement {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return nil
	}

	length := zs.zsl.length
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}
	if start < 0 {
		start = 0
	}
	if stop < 0 {
		stop = 0
	}

	if start > stop || start >= length {
		return nil
	}
	if stop >= length {
		stop = length - 1
	}

	node := zs.zsl.getNodeByRank(length - start)
	if node == nil {
		return nil
	}

	result := make([]ZSetElement, 0, stop-start+1)
	for i := start; i <= stop && node != nil; i++ {
		result = append(result, ZSetElement{Member: node.ele, Score: node.score})
		node = node.backward
	}
	return result
}

func (s *ZSetStore) ZCount(key string, min, max float64) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, ok := s.data[key]
	if !ok {
		return 0
	}

	count := 0
	node := zs.zsl.header.level[0].forward
	for node != nil {
		if node.score >= min && node.score <= max {
			count++
		} else if node.score > max {
			break
		}
		node = node.level[0].forward
	}
	return count
}
