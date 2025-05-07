package db

type KType uint32

const (
	// Zero is reserved for preallocated files
	kZeroType KType = 0
	kFullType KType = 1
	// For fragments
	kFirstType  KType = 2
	kMiddleType KType = 3
	kLastType   KType = 4
	kEof        KType = 5
	kBadRecord  KType = 6
)

const (
	kMaxRecordType = uint32(kLastType)

	kBlockSize uint32 = 32768
	// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
	kHeaderSize uint32 = 4 + 2 + 1
)
