package db

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmptySkipList(t *testing.T) {
	list := NewSkipList[uint64](&_IntCompartor[uint64]{})

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

	list := NewSkipList[uint64](&_IntCompartor[uint64]{})
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
