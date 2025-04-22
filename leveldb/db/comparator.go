package db

import (
	"bytes"
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
type _SliceComparator[T Slice] struct{}

func (*_SliceComparator[T]) Compare(a, b T) int {
	// Notice: a nil argument is equivalent to an empty slice using bytes.Compare.
	return bytes.Compare(a, b)
}

func (*_SliceComparator[T]) Name() string {
	return "leveldb.bytesComparator"
}

type SliceComparator _SliceComparator[Slice]

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
