package db

import (
	"math/rand"
	"time"
)

const (
	skipListMaxHeight = 12
)

/*
	1. head的key是nil，不能使用head的key
	2. 如果node为nil，则认为这个node包含最大的key，即node为右边界
*/

// TODO: Thread-Safe
// TODO: 先实现 Insert 和 Contain，然后跑 UT

type SkipList[T KeyTypeSet] struct {
	rnd           rand.Source
	cmp           Compartor[T]
	head          *SkipListNode[T]
	currentHeight int32
}

func NewSkipList[T KeyTypeSet](compartor Compartor[T]) *SkipList[T] {
	return &SkipList[T]{
		rnd:           rand.NewSource(time.Now().UnixNano()),
		cmp:           compartor,
		head:          NewSkipListNode[T](skipListMaxHeight, nil),
		currentHeight: 1,
	}
}

// Insert
// REQUIRES: nothing that compares equal to key is currently in the list.
func (s *SkipList[T]) Insert(key *T) {
	prevNodes := make([]*SkipListNode[T], skipListMaxHeight, skipListMaxHeight)
	_ = s.findGreaterOrEqual(key, prevNodes)

	height := s.randomHeight()
	newNode := NewSkipListNode(height, key)

	if height > s.currentHeight {
		for i := s.currentHeight; i < height; i++ {
			prevNodes[i] = s.head
		}
		s.currentHeight = height
	}
	for i := 0; i < int(height); i++ {
		newNode.next[i] = prevNodes[i].next[i]
		prevNodes[i].next[i] = newNode
	}
}

// Contains returns true iff an entry that compares equal to key is in the list.
func (s *SkipList[T]) Contains(key *T) bool {
	x := s.findGreaterOrEqual(key, nil)
	return x != nil && s.cmp.Compare(x.key, key) == 0
}

func (s *SkipList[T]) findLessThan(key *T) *SkipListNode[T] {
	x := s.head
	level := s.currentHeight - 1
	for {
		next := x.next[level]
		if next != nil && s.cmp.Compare(next.key, key) < 0 {
			x = next
		} else {
			if level == 0 {
				return x
			} else {
				level--
			}
		}
	}
}

func (s *SkipList[T]) findGreaterOrEqual(key *T, prevNodes []*SkipListNode[T]) *SkipListNode[T] {
	x := s.head
	level := s.currentHeight - 1
	for {
		next := x.next[level]
		if s.keyIsAfterNode(key, next) {
			x = next
		} else {
			if prevNodes != nil {
				prevNodes[level] = x
			}
			if level == 0 {
				return next
			} else {
				level--
			}
		}
	}
}

func (s *SkipList[T]) findLast() *SkipListNode[T] {
	level := s.currentHeight
	x := s.head
	for {
		next := x.next[level]
		if next != nil {
			x = next
		} else if level > 0 {
			level--
		} else {
			return x
		}
	}
}

func (s *SkipList[T]) keyIsAfterNode(key *T, node *SkipListNode[T]) bool {
	if node == nil {
		return false
	}
	return s.cmp.Compare(key, node.key) > 0
}

func (s *SkipList[T]) randomHeight() int32 {
	const skipListBranching = 4
	height := int32(1)
	for height < skipListMaxHeight && s.rnd.Int63()%skipListBranching == 0 {
		height += 1
	}
	return height
}

// SkipListIterator Iteration over the contents of a skip list
type SkipListIterator[T KeyTypeSet] struct {
	list *SkipList[T]
	node *SkipListNode[T]
}

func NewSkipListIterator[T KeyTypeSet](list *SkipList[T]) *SkipListIterator[T] {
	return &SkipListIterator[T]{
		list: list,
		node: nil,
	}
}

// Valid Returns true iff the iterator is positioned at a valid node.
func (iter *SkipListIterator[T]) Valid() bool {
	return iter.node != nil
}

// GetKey Returns the key at the current position.
// REQUIRES: Valid()
func (iter *SkipListIterator[T]) GetKey() *T {
	return iter.node.key
}

// Next Advances to the next position.
// REQUIRES: Valid()
func (iter *SkipListIterator[T]) Next() {
	iter.node = iter.node.next[0]
}

// Prev Advances to the previous position.
// REQUIRES: Valid()
func (iter *SkipListIterator[T]) Prev() {
	// Instead of using explicit "prev" links, we just search for the
	// last node that falls before key.
	iter.node = iter.list.findLessThan(iter.node.key)
	if iter.node == iter.list.head {
		iter.node = nil
	}
}

// Seek Advance to the first entry with a key >= target
func (iter *SkipListIterator[T]) Seek(target *T) {
	iter.node = iter.list.findGreaterOrEqual(target, nil)
}

// SeekToFirst Position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (iter *SkipListIterator[T]) SeekToFirst() {
	iter.node = iter.list.head.next[0]
}

// SeekToLast Position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (iter *SkipListIterator[T]) SeekToLast() {
	iter.node = iter.list.findLast()
	if iter.node == iter.list.head {
		iter.node = nil
	}
}

type SkipListNode[T KeyTypeSet] struct {
	next []*SkipListNode[T]
	key  *T
}

func NewSkipListNode[T KeyTypeSet](height int32, key *T) *SkipListNode[T] {
	return &SkipListNode[T]{
		next: make([]*SkipListNode[T], height, height),
		key:  key,
	}
}
