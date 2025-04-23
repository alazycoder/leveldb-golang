package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleReadWrite(t *testing.T) {
	memTable := NewMemTable()

	var valueType ValueType
	var value Slice

	valueType, _ = memTable.Get(NewLookupKey([]byte("foo"), 1))
	assert.Equal(t, valueTypeNotExist, valueType)

	memTable.Add(1, valueTypeValue, []byte("foo"), []byte("v1"))
	valueType, value = memTable.Get(NewLookupKey([]byte("foo"), 1))
	assert.Equal(t, valueTypeValue, valueType)
	assert.Equal(t, []byte("v1"), []byte(value))

	memTable.Add(2, valueTypeValue, []byte("foo"), []byte(""))
	valueType, value = memTable.Get(NewLookupKey([]byte("foo"), 2))
	assert.Equal(t, valueTypeValue, valueType)
	assert.Equal(t, []byte(""), []byte(value))

	memTable.Add(3, valueTypeValue, []byte("bar"), []byte("v2"))
	valueType, value = memTable.Get(NewLookupKey([]byte("bar"), 3))
	assert.Equal(t, valueTypeValue, valueType)
	assert.Equal(t, []byte("v2"), []byte(value))

	memTable.Add(4, valueTypeDeletion, []byte("foo"), []byte(""))
	valueType, _ = memTable.Get(NewLookupKey([]byte("foo"), 4))
	assert.Equal(t, valueTypeDeletion, valueType)

	memTable.Add(5, valueTypeDeletion, []byte("bar"), []byte(""))
	valueType, _ = memTable.Get(NewLookupKey([]byte("bar"), 5))
	assert.Equal(t, valueTypeDeletion, valueType)
}
