package util

import "hash/crc32"

func Crc32Value(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func Crc32ValueWithInitial(initial uint32, data []byte) uint32 {
	return crc32.Update(initial, crc32.IEEETable, data)
}
