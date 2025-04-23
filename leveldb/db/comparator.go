package db

import (
	"bytes"
	"unsafe"

	"leveldb-golang/leveldb/util"
)

type IntKeyTypeSet interface {
	~int32 | ~uint32 | ~int64 | ~uint64
}

type KeyTypeSet interface {
	IntKeyTypeSet | ~[]byte
}

// Comparator must be thread-safe
type Comparator[T KeyTypeSet] interface {

	// Compare < 0 iff "a" < "b", == 0 iff "a" == "b", > 0 iff "a" > "b"
	Compare(a, b *T) int

	// Name of the comparator. Prevent mismatch (i.e., a database created with one comparator and accessed with another
	// comparator)
	Name() string

	// TODO: implement two advanced functions: FindShortestSeparator and FindShortSuccessor
}

type Slice []byte

type UserKeyComparator[T Slice] struct{}

func (*UserKeyComparator[T]) Compare(a, b *T) int {
	// Notice: a nil argument is equivalent to an empty slice using bytes.Compare.
	return bytes.Compare(*a, *b)
}

func (*UserKeyComparator[T]) Name() string {
	return "leveldb.userKeyComparator"
}

func NewUserKeyComparator[T Slice]() *UserKeyComparator[T] {
	return &UserKeyComparator[T]{}
}

type InternalKeyCompartor[T Slice] struct {
	userKeyComparator *UserKeyComparator[T]
}

func NewInternalKeyCompartor[T Slice](comparator *UserKeyComparator[T]) *InternalKeyCompartor[T] {
	return &InternalKeyCompartor[T]{
		userKeyComparator: comparator,
	}
}

func (c *InternalKeyCompartor[T]) Compare(a, b *T) int {
	aUserKey := ExtractUserKey((Slice)(*a))
	bUserKey := ExtractUserKey((Slice)(*b))

	r := c.userKeyComparator.Compare((*T)(unsafe.Pointer(&aUserKey)), (*T)(unsafe.Pointer(&bUserKey)))
	if r == 0 {
		aTag := util.DecodeFixedUint64((Slice)(*a)[len(aUserKey):])
		bTag := util.DecodeFixedUint64((Slice)(*b)[len(bUserKey):])
		if aTag > bTag {
			return -1
		} else if aTag < bTag {
			return 1
		} else {
			return 0
		}
	}
	return r
}

func (*InternalKeyCompartor[T]) Name() string {
	return "leveldb.internalKeyComparator"
}

type MemTableKeyCompartor[T Slice] struct {
	internalKeyCompartor *InternalKeyCompartor[T]
}

func NewMemTableKeyCompartor[T Slice](compartor *InternalKeyCompartor[T]) *MemTableKeyCompartor[T] {
	return &MemTableKeyCompartor[T]{
		internalKeyCompartor: compartor,
	}
}

func (c *MemTableKeyCompartor[T]) Compare(a, b *T) int {
	aSlice := GetLengthPrefixedSlice(*a)
	bSlice := GetLengthPrefixedSlice(*b)
	return c.internalKeyCompartor.Compare((*T)(unsafe.Pointer(&aSlice)), (*T)(unsafe.Pointer(&bSlice)))
}

func (c *MemTableKeyCompartor[T]) Name() string {
	return "leveldb.memTableKeyComparator"
}

type _IntComparator[T IntKeyTypeSet]struct{}

func (t *_IntComparator[T]) Compare(a, b *T) int {
	if *a < *b {
		return -1
	} else if *a == *b {
		return 0
	} else {
		return 1
	}
}

func (t *_IntComparator[T]) Name() string {
	return "leveldb.intComparator"
}

type UInt64Comparator _IntComparator[uint64]
