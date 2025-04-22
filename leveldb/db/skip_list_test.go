package db

import (
	"encoding/binary"
	"math/rand"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"leveldb-golang/leveldb/util"
)

func TestEmptySkipList(t *testing.T) {
	list := NewSkipList[uint64](&_IntComparator[uint64]{})

	key := uint64(10)
	assert.Equal(t, false, list.Contains(&key))

	iter := NewSkipListIterator(list)
	assert.Equal(t, false, iter.Valid())

	iter.SeekToFirst()
	assert.Equal(t, false, iter.Valid())

	key = 100
	iter.Seek(&key)
	assert.Equal(t, false, iter.Valid())

	iter.SeekToLast()
	assert.Equal(t, false, iter.Valid())
}

func TestInsertAndLookup(t *testing.T) {
	N := 2000
	R := 5000

	rnd := rand.NewSource(1000)
	m := make(map[uint64]bool)

	list := NewSkipList[uint64](&_IntComparator[uint64]{})
	for i := 0; i < N; i++ {
		key := uint64(rnd.Int63() % int64(R))
		if !m[key] {
			m[key] = true
			list.Insert(&key)
		}
	}

	for i := 0; i < R; i++ {
		key := uint64(i)
		assert.Equalf(t, m[key], list.Contains(&key), "key = %d", key)
	}

	keys := make([]uint64, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	iter := NewSkipListIterator(list)
	assert.Equal(t, false, iter.Valid())

	key := uint64(0)
	iter.Seek(&key)
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, keys[0], *iter.GetKey())

	iter.SeekToFirst()
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, keys[0], *iter.GetKey())

	iter.SeekToLast()
	assert.Equal(t, true, iter.Valid())
	assert.Equal(t, keys[len(keys)-1], *iter.GetKey())

	// Forward iteration test
	idx := 0
	for i := 0; i < R; i++ {
		iter = NewSkipListIterator(list)
		key = uint64(i)
		iter.Seek(&key)

		for idx < len(keys) && keys[idx] < key {
			idx += 1
		}
		for j := 0; j < 3; j++ {
			if idx+j >= len(keys) {
				assert.Equal(t, false, iter.Valid())
				break
			} else {
				assert.Equal(t, true, iter.Valid())
				assert.Equal(t, keys[idx+j], *iter.GetKey())
				iter.Next()
			}
		}
	}

	// Backward iteration test
	iter = NewSkipListIterator(list)
	iter.SeekToLast()
	for i := len(keys) - 1; i >= 0; i-- {
		assert.Equal(t, true, iter.Valid())
		assert.Equal(t, keys[i], *iter.GetKey())
		iter.Prev()
	}
	assert.Equal(t, false, iter.Valid())
}

// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructed.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
func TestWithoutConcurrent(t *testing.T) {
	ct := NewConcurrentTest(t)
	randSource := rand.NewSource(0)
	for i := 0; i < 10000; i++ {
		ct.readStep(randSource)
		ct.writeStep(randSource)
	}
}

func TestConcurrent(t *testing.T) {
	runConcurrent(t, 1)
	runConcurrent(t, 2)
	runConcurrent(t, 3)
	runConcurrent(t, 4)
	runConcurrent(t, 5)
}

func runConcurrent(t *testing.T, runId int64) {
	const N = 1000
	const kSize = 1000

	t.Logf("runConcurrent: %d", runId)

	seed := runId * 100
	for i := 0; i < N; i++ {
		if i%100 == 0 {
			t.Logf("Run %d of %d", i, N)
		}

		testState := NewTestState(t, seed)

		go func() {
			testState.runningChan <- struct{}{}
			src := rand.NewSource(seed + 1)
			for atomic.LoadUint32(&testState.quitFlag) == 0 {
				testState.ct.readStep(src)
			}
			testState.doneChan <- struct{}{}
		}()

		<-testState.runningChan
		src := rand.NewSource(seed)
		for k := 0; k < kSize; k++ {
			testState.ct.writeStep(src)
		}
		atomic.StoreUint32(&testState.quitFlag, 1)
		<-testState.doneChan
	}
}

type TestState struct {
	ct          *ConcurrentTest
	seed        int64
	quitFlag    uint32
	runningChan chan struct{}
	doneChan    chan struct{}
}

func NewTestState(t *testing.T, seed int64) *TestState {
	return &TestState{
		ct:          NewConcurrentTest(t),
		seed:        seed,
		quitFlag:    0,
		runningChan: make(chan struct{}),
		doneChan:    make(chan struct{}),
	}
}

const K uint64 = 4

type ConcurrentTest struct {
	t        *testing.T
	genState *GenerationState
	list     *SkipList[uint64]
}

func NewConcurrentTest(t *testing.T) *ConcurrentTest {
	return &ConcurrentTest{
		t:        t,
		genState: NewGenerationState(),
		list:     NewSkipList[uint64](&_IntComparator[uint64]{}),
	}
}

func (*ConcurrentTest) key(key uint64) uint64 {
	return key >> 40
}

func (*ConcurrentTest) gen(key uint64) uint64 {
	return (key >> 8) & (0xffffffff)
}

func (*ConcurrentTest) hash(key uint64) uint64 {
	return key & (0xff)
}

func (*ConcurrentTest) hashNumbers(k, g uint64) uint64 {
	data := make([]byte, 16, 16)
	binary.LittleEndian.PutUint64(data, k)
	binary.LittleEndian.PutUint64(data[8:], g)
	return uint64(util.Hash(data, 0))
}

func (ct *ConcurrentTest) makeKey(k, g uint64) uint64 {
	assert.LessOrEqual(ct.t, k, K)
	assert.LessOrEqual(ct.t, g, uint64(0xffffffff))
	return (k << 40) | (g << 8) | (ct.hashNumbers(k, g) & (0xff))
}

func (ct *ConcurrentTest) isValidKey(key uint64) bool {
	return (ct.hashNumbers(ct.key(key), ct.gen(key)) & 0xff) == ct.hash(key)
}

func (ct *ConcurrentTest) randomTarget(src rand.Source) uint64 {
	switch src.Int63() % 10 {
	case 0:
		// Seek to beginning
		return ct.makeKey(0, 0)
	case 1:
		// Seek to end
		return ct.makeKey(K, 0)
	default:
		// Seek to middle
		return ct.makeKey(uint64(src.Int63())%K, 0)
	}
}

func (ct *ConcurrentTest) writeStep(src rand.Source) {
	k := uint64(src.Int63()) % K
	g := ct.genState.Get(k) + 1
	key := ct.makeKey(k, g)
	ct.list.Insert(&key)
	ct.genState.Set(k, g)
}

func (ct *ConcurrentTest) readStep(src rand.Source) {
	initialState := NewGenerationState()
	for k := uint64(0); k < K; k++ {
		initialState.Set(k, ct.genState.Get(k))
	}

	pos := ct.randomTarget(src)
	iterator := NewSkipListIterator(ct.list)
	iterator.Seek(&pos)
	for {
		var current uint64
		if !iterator.Valid() {
			current = ct.makeKey(K, 0)
		} else {
			current = *iterator.GetKey()
			assert.True(ct.t, ct.isValidKey(current))
		}
		assert.LessOrEqual(ct.t, pos, current)

		// Verify that everything in [pos,current) was not present in initialState.
		for pos < current {
			assert.Less(ct.t, ct.key(pos), K)

			assert.True(ct.t, ct.gen(pos) == 0 || ct.gen(pos) > initialState.Get(ct.key(pos)))

			// Advance to next key in the valid key space
			if ct.key(pos) < ct.key(current) {
				pos = ct.makeKey(ct.key(pos)+1, 0)
			} else {
				pos = ct.makeKey(ct.key(pos), ct.gen(pos)+1)
			}
		}

		if !iterator.Valid() {
			break
		}

		if src.Int63()%2 == 0 {
			iterator.Next()
			pos = ct.makeKey(ct.key(pos), ct.gen(pos)+1)
		} else {
			newPos := ct.randomTarget(src)
			if newPos > pos {
				pos = newPos
				iterator.Seek(&newPos)
			}
		}
	}
}

type GenerationState struct {
	generation []uint64
}

func NewGenerationState() *GenerationState {
	return &GenerationState{
		generation: make([]uint64, 4, 4),
	}
}

func (s *GenerationState) Set(k, v uint64) {
	atomic.StoreUint64(&s.generation[k], v)
}

func (s *GenerationState) Get(k uint64) uint64 {
	return atomic.LoadUint64(&s.generation[k])
}
