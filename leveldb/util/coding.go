package util

import "encoding/binary"

func EncodeVarInt32(data []byte, value uint32) {
	// 对于每个byte b，如果b的最高位为1，则说明后边还有其他byte，如果b的最高位为0，则说明这是最后一个byte
	// little endian
	const B uint8 = 128
	if value < (1 << 7) {
		data[0] = uint8(value)
	} else if value < (1 << 14) {
		data[0] = uint8(value) | B
		data[1] = uint8(value >> 7)
	} else if value < (1 << 21) {
		data[0] = uint8(value) | B
		data[1] = uint8(value>>7) | B
		data[2] = uint8(value >> 14)
	} else if value < (1 << 28) {
		data[0] = uint8(value) | B
		data[1] = uint8(value>>7) | B
		data[2] = uint8(value>>14) | B
		data[3] = uint8(value >> 21)
	} else {
		data[0] = uint8(value) | B
		data[1] = uint8(value>>7) | B
		data[2] = uint8(value>>14) | B
		data[3] = uint8(value>>21) | B
		data[4] = uint8(value >> 28)
	}
}

func EncodeFixedUint64(data []byte, value uint64) {
	binary.LittleEndian.PutUint64(data, value)
}

func DecodeFixedUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func EncodeFixedUint32(data []byte, value uint32) {
	binary.LittleEndian.PutUint32(data, value)
}

func DecodeFixedUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func VarIntLength(value uint64) uint32 {
	length := uint32(1)
	for value >= 128 {
		value >>= 7
		length++
	}
	return length
}

func DecodeVarInt32(data []byte) (value uint32, size uint32) {
	shift := 0
	for i := 0; i < len(data) && shift <= 28; i++ {
		value |= uint32(data[i]&127) << shift
		size += 1
		if (data[i] & 128) == 0 {
			return value, size
		}
		shift += 7
	}
	return value, size
}
