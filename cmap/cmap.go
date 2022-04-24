package cmap

import (
	"encoding/binary"
	"encoding/json"
	"sync"
)

var ShardCount = 32

type ConcurrentMap []*ConcurrentMapShared

type ConcurrentMapShared struct {
	items        map[int64]interface{}
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

func New() ConcurrentMap {
	m := make(ConcurrentMap, ShardCount)
	for i := 0; i < ShardCount; i++ {
		m[i] = &ConcurrentMapShared{items: make(map[int64]interface{})}
	}
	return m
}

func (m ConcurrentMap) GetShard(key int64) *ConcurrentMapShared {
	return m[uint(fnv32(key))%uint(ShardCount)]
}

func (m ConcurrentMap) MSet(data map[int64]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

func (m ConcurrentMap) Set(key int64, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

func (m ConcurrentMap) Upsert(key int64, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

func (m ConcurrentMap) SetIfAbsent(key int64, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

func (m ConcurrentMap) Get(key int64) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

func (m ConcurrentMap) Count() int {
	count := 0
	for i := 0; i < ShardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m ConcurrentMap) Has(key int64) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

func (m ConcurrentMap) Remove(key int64) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

type RemoveCb func(key int64, v interface{}, exists bool) bool

func (m ConcurrentMap) RemoveCb(key int64, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

func (m ConcurrentMap) Pop(key int64) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcurrentMap) IsEmpty() bool {
	return m.Count() == 0
}

type Tuple struct {
	Key int64
	Val interface{}
}

func (m ConcurrentMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

func (m ConcurrentMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

func (m ConcurrentMap) Clear() {
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

func snapshot(m ConcurrentMap) (chans []chan Tuple) {
	if len(m) == 0 {
		panic(`cmap.ConcurrentMap is not initialized. Should run New() before usage.`)
	}
	chans = make([]chan Tuple, ShardCount)
	wg := sync.WaitGroup{}
	wg.Add(ShardCount)
	// Foreach shard.
	for index, shard := range m {
		go func(index int, shard *ConcurrentMapShared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

func (m ConcurrentMap) Items() map[int64]interface{} {
	tmp := make(map[int64]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

type IterCb func(key int64, v interface{})

func (m ConcurrentMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

func (m ConcurrentMap) Keys() []int64 {
	count := m.Count()
	ch := make(chan int64, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(ShardCount)
		for _, shard := range m {
			go func(shard *ConcurrentMapShared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	keys := make([]int64, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

func (m ConcurrentMap) MarshalJSON() ([]byte, error) {
	tmp := make(map[int64]interface{})

	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

func fnv32(key int64) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	var bytes = make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(key))
	keyLength := len(bytes)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(bytes[i])
	}
	return hash
}
