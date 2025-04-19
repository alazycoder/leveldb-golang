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

// Compartor must be thread-safe
type Compartor[T KeyTypeSet] interface {

	// Compare < 0 iff "a" < "b", == 0 iff "a" == "b", > 0 iff "a" > "b"
	Compare(a, b *T) int

	// Name of the compartor. Prevent mismatch (i.e., a database created with one compartor and accessed with another
	// compartor)
	Name() string

	// TODO: implement two advanced functions: FindShortestSeparator and FindShortSuccessor
}

type Slice []byte
type _SliceCompartor[T Slice] struct{}

func (*_SliceCompartor[T]) Compare(a, b T) int {
	// Notice: a nil argument is equivalent to an empty slice using bytes.Compare.
	return bytes.Compare(a, b)
}

func (*_SliceCompartor[T]) Name() string {
	return "leveldb.bytesCompartor"
}

type SliceCompartor _SliceCompartor[Slice]

type _IntCompartor[T IntKeyTypeSet]struct{}

func (t *_IntCompartor[T]) Compare(a, b *T) int {
	if *a < *b {
		return -1
	} else if *a == *b {
		return 0
	} else {
		return 1
	}
}

func (t *_IntCompartor[T]) Name() string {
	return "leveldb.intCompartor"
}

type UInt64Compartor _IntCompartor[uint64]
