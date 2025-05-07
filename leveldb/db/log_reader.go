package db

import (
	"errors"
	"io"

	"leveldb-golang/leveldb/util"
)

type Reporter interface {
	Corruption(n uint32, err error)
}

type LogReader struct {
	source            io.ReadSeeker
	reporter          Reporter
	checksum          bool
	backingStore      []byte
	buffer            []byte
	eof               bool
	lastRecordOffset  uint32
	endOfBufferOffset uint32
	initialOffset     uint32
	resyncing         bool // 表示Reader是否处于跳过开头不完整块，正在寻找第一个合法的（kFirstType kFullType）起始block的阶段
}

func NewLogReader(source io.ReadSeeker, reporter Reporter, checksum bool, initialOffset uint32) *LogReader {
	reader := &LogReader{
		source:        source,
		reporter:      reporter,
		checksum:      checksum,
		backingStore:  make([]byte, kBlockSize), // 重复利用，防止多次空间申请和释放
		initialOffset: initialOffset,
	}
	if initialOffset > 0 {
		reader.resyncing = true
	}
	return reader
}

// SkipToInitialBlock
// 找到第一个应该读取的Block
func (lr *LogReader) SkipToInitialBlock() bool {
	offsetInBlock := lr.initialOffset % kBlockSize
	blockStartLocation := lr.initialOffset - offsetInBlock

	// 当前 block 剩下的空间不足以放下一个 header，写的时候会用 0 填充，没必要读
	if offsetInBlock > kBlockSize-kHeaderSize {
		blockStartLocation += kBlockSize
	}

	if blockStartLocation > 0 {
		lr.endOfBufferOffset = blockStartLocation
		_, err := lr.source.Seek(int64(blockStartLocation), io.SeekStart)
		if err != nil {
			lr.reporter.Corruption(
				blockStartLocation,
				util.NewLevelDbError(util.ErrSeekFileFailed,
					"failed to seek file to offset %d, error: %v", blockStartLocation, err))
			return false
		}
	}

	return true
}

// ReadRecord
// 第一个返回值表示本次读取到的 Slice，只有当第二个返回值为 true 时才有意义
// 第二个返回值表示本次是否读取到了 Slice
func (lr *LogReader) ReadRecord() (Slice, bool) {
	if lr.lastRecordOffset < lr.initialOffset {
		if !lr.SkipToInitialBlock() {
			return nil, false
		}
	}

	record := make([]byte, 0)
	inFragmentRecord := false
	tmpLastRecordOffset := lr.lastRecordOffset

	for {
		fragment, fragmentType := lr.ReadPhysicalRecord()
		physicalRecordOffset := lr.endOfBufferOffset - uint32(len(lr.buffer)) - kHeaderSize - uint32(len(fragment))

		if lr.resyncing {
			if fragmentType == kMiddleType {
				continue
			} else if fragmentType == kLastType {
				lr.resyncing = false
				continue
			} else {
				lr.resyncing = false
			}
		}

		switch fragmentType {
		case kFullType:
			if inFragmentRecord {
				// 把前面已经读到的数据丢弃，把当前这些数据返回
				if len(record) > 0 {
					lr.reporter.Corruption(
						uint32(len(record)),
						util.NewLevelDbError(util.ErrPartialRecordWithoutEnd, "partial record without end(1)"),
					)
				}
				record = record[:0]
			}
			lr.lastRecordOffset = physicalRecordOffset
			record = append(record, fragment...)
			return record, true
		case kFirstType:
			if inFragmentRecord {
				// 把前面已经读到的数据丢弃，从当前的fragment开始重新读
				if len(record) > 0 {
					lr.reporter.Corruption(
						uint32(len(record)),
						util.NewLevelDbError(util.ErrPartialRecordWithoutEnd, "partial record without end(2)"),
					)
				}
				record = record[:0]
			}
			tmpLastRecordOffset = physicalRecordOffset // 这里先不更新 lr.lastRecordOffset，只临时保存
			record = append(record, fragment...)
			inFragmentRecord = true
			// 这里不能继续返回，需要继续往下读
		case kMiddleType:
			if !inFragmentRecord {
				lr.reporter.Corruption(
					uint32(len(fragment)),
					util.NewLevelDbError(util.ErrMissingStart, "missing start of fragmented record(1)"),
				)
				record = record[:0] // 丢弃前面的数据
			} else {
				record = append(record, fragment...)
			}
			// 这里不能继续返回，需要继续往下读
		case kLastType:
			if !inFragmentRecord {
				lr.reporter.Corruption(
					uint32(len(fragment)),
					util.NewLevelDbError(util.ErrMissingStart, "missing start of fragmented record(2)"),
				)
				record = record[:0] // 丢弃前面的数据
				// 这里不能继续返回，需要继续往下读
			} else {
				lr.lastRecordOffset = tmpLastRecordOffset // 更新 lr.lastRecordOffset
				record = append(record, fragment...)
				return record, true
			}
		case kEof:
			return nil, false
		case kBadRecord:
			if inFragmentRecord {
				lr.reporter.Corruption(
					uint32(len(record)),
					util.NewLevelDbError(util.ErrInMiddleRecord, "error in middle of record"),
				)
				inFragmentRecord = false
				record = record[:0] // 丢弃前面的数据
			}
			// 这里不能继续返回，需要继续往下读
		default:
			lr.reporter.Corruption(
				uint32(len(fragment)+len(record)),
				util.NewLevelDbError(util.ErrUnknownRecordType, "invalid record type %d", fragmentType),
			)
			inFragmentRecord = false
			record = record[:0]
		}
	}
}

// ReadPhysicalRecord
// 读取一个新的 fragment，返回 data 和 type
func (lr *LogReader) ReadPhysicalRecord() (Slice, KType) {
	if uint32(len(lr.buffer)) < kHeaderSize {
		if !lr.eof {
			// 上一次 block 剩余空间不足以写下一个 header，用 0 填充了
			n, err := lr.source.Read(lr.backingStore)
			if errors.Is(err, io.EOF) {
				lr.eof = true
			} else if err != nil {
				lr.eof = true
				lr.reporter.Corruption(
					kBlockSize,
					util.NewLevelDbError(util.ErrReadFileFailed, "failed to read file, error: [%v]", err),
				)
				return nil, kEof
			}
			if uint32(n) < kBlockSize {
				lr.eof = true
			}

			lr.endOfBufferOffset += uint32(n)
			lr.buffer = lr.backingStore[:n]
		}
	}

	if uint32(len(lr.buffer)) < kHeaderSize {
		return nil, kEof
	}

	length := uint32(lr.buffer[4]) | (uint32(lr.buffer[5]) << 8)
	kType := KType(lr.buffer[6])

	if kHeaderSize+length > uint32(len(lr.buffer)) {
		dropSize := uint32(len(lr.buffer))
		lr.buffer = lr.buffer[:0]

		if !lr.eof {
			lr.reporter.Corruption(
				dropSize,
				util.NewLevelDbError(util.ErrBadRecordLength, "expected length: %d, actual length: %d",
					length, dropSize-kHeaderSize))
			return nil, kBadRecord
		}

		// log writer 非正常终止，可能留下未写完的 record
		return nil, kEof
	}

	if lr.checksum {
		expectedCrc := util.DecodeFixedUint32(lr.buffer[:4])
		actualCrc := util.Crc32Value(lr.buffer[6 : 7+length])
		if expectedCrc != actualCrc {
			// 将 buffer 中所有的数据都丢弃，crc 不 match，有可能是数据部分损坏了，也有可能是 length 损坏了
			dropSize := uint32(len(lr.buffer))
			lr.buffer = lr.buffer[:0]
			lr.reporter.Corruption(
				dropSize,
				util.NewLevelDbError(util.ErrCheckCrcFailed, "expect crc: %d, actual crc: %d",
					expectedCrc, actualCrc),
			)
			return nil, kBadRecord
		}
	}

	defer func() {
		lr.buffer = lr.buffer[kHeaderSize+length:]
	}()

	if lr.endOfBufferOffset-uint32(len(lr.buffer)) < lr.initialOffset {
		return nil, kBadRecord
	}

	return lr.buffer[kHeaderSize : kHeaderSize+length], kType
}

func (lr *LogReader) LastRecordOffset() uint32 {
	return lr.lastRecordOffset
}
