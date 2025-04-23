package db

import (
	"leveldb-golang/leveldb/util"
)

type ValueType uint8
type SequenceNumber uint64

const (
	// 注意: valueTypeDeletion 必须比 valueTypeValue 的值小
	// 因为在 skiplist 中，internalKey相同时，按 tag 从大到小排序
	// 查询时，LookupKey 中用的是 valueTypeValue
	// 如果某个 seq 执行的是删除操作，以这个 seq 查询时，要能 Seek 到这条删除记录
	valueTypeDeletion ValueType = iota
	valueTypeValue
	valueTypeNotExist
)

type LookupKey struct {
	data            Slice
	userKeyStartIdx uint32
}

func NewLookupKey(userKey Slice, seq SequenceNumber) *LookupKey {
	internalKeySize := uint32(len(userKey)) + 8
	internalKeySizeLength := util.VarIntLength(uint64(internalKeySize))
	totalLength := internalKeySizeLength + uint32(len(userKey)) + 8

	data := make(Slice, totalLength, totalLength)
	util.EncodeVarInt32(data, internalKeySize)
	copy(data[internalKeySizeLength:], userKey)
	util.EncodeFixedUint64(data[internalKeySizeLength+uint32(len(userKey)):], uint64(seq<<8)|uint64(valueTypeValue))

	return &LookupKey{
		data:            data,
		userKeyStartIdx: internalKeySizeLength,
	}
}

func (key *LookupKey) MemTableKey() Slice {
	return key.data
}

func (key *LookupKey) UserKey() Slice {
	return key.data[key.userKeyStartIdx : len(key.data)-8]
}

func ExtractUserKey(internalKey Slice) Slice {
	return internalKey[:len(internalKey)-8]
}
