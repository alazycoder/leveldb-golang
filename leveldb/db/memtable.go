package db

import (
	"leveldb-golang/leveldb/util"
)

type MemTable struct {
	table *SkipList[Slice]

	userKeyComparator     *UserKeyComparator[Slice]
	internalKeyComparator *InternalKeyCompartor[Slice]
}

func NewMemTable() *MemTable {
	userKeyComparator := NewUserKeyComparator[Slice]()
	internalKeyComparator := NewInternalKeyCompartor[Slice](userKeyComparator)
	memTableKeyComparator := NewMemTableKeyCompartor[Slice](internalKeyComparator)

	return &MemTable{
		table:                 NewSkipList[Slice](memTableKeyComparator),
		userKeyComparator:     userKeyComparator,
		internalKeyComparator: internalKeyComparator,
	}
}

func (mem *MemTable) Add(seq SequenceNumber, valueType ValueType, key, value Slice) {
	// Format of an entry is concatenation of:
	//  key_size     : varint32 of internal_key.size()
	//  key bytes    : char[internal_key.size()]
	//  tag          : uint64((sequence << 8) | type)
	//  value_size   : varint32 of value.size()
	//  value bytes  : char[value.size()]
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	internalKeySize := keySize + 8
	internalKeySizeLength := util.VarIntLength(uint64(internalKeySize))
	valueSizeLength := util.VarIntLength(uint64(valueSize))
	totalLength := internalKeySizeLength + internalKeySize + valueSizeLength + valueSize

	data := make(Slice, totalLength, totalLength)

	util.EncodeVarInt32(data, internalKeySize)
	currentLength := internalKeySizeLength
	copy(data[currentLength:], key)
	currentLength += keySize
	util.EncodeFixedUint64(data[currentLength:], uint64(seq<<8)|uint64(valueType))
	currentLength += 8
	util.EncodeVarInt32(data[currentLength:], valueSize)
	currentLength += valueSizeLength
	copy(data[currentLength:], value)

	mem.table.Insert(&data)
}

// Get If mem contains a value for key, return (valueTypeValue, value)
// If mem contains a deletion for key, return (valueTypeDeletion, nil)
// Else return (valueTypeNotExist, nil)
func (mem *MemTable) Get(lookupKey *LookupKey) (ValueType, Slice) {
	memTableKey := lookupKey.MemTableKey()
	iterator := NewSkipListIterator(mem.table)
	iterator.Seek(&memTableKey)
	if iterator.Valid() {
		// SkipList中的元素排序方式：先按 []byte(varint32) + []byte(userKey) 升序排序，
		// 再按 sequence number 降序排序
		// Seek 已经过滤掉了前缀相同但 sequence number 更大的元素了，所以不会读到后边插入的值
		entry := iterator.GetKey()
		keyLength, keyLengthSize := util.DecodeVarInt32(*entry)
		userKey := lookupKey.UserKey()
		userKeyInEntry := (*entry)[keyLengthSize : keyLengthSize+keyLength-8]
		if mem.userKeyComparator.Compare(&userKey, &userKeyInEntry) == 0 {
			tag := util.DecodeFixedUint64((*entry)[keyLengthSize+keyLength-8:])
			switch ValueType(tag & 0xff) {
			case valueTypeValue:
				return valueTypeValue, GetLengthPrefixedSlice((*entry)[keyLengthSize+keyLength:])
			case valueTypeDeletion:
				return valueTypeDeletion, nil
			}
		}
	}

	return valueTypeNotExist, nil
}

func GetLengthPrefixedSlice(data []byte) []byte {
	length, lengthSize := util.DecodeVarInt32(data)
	return data[lengthSize : lengthSize+length]
}
