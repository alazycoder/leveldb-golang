package db

import (
	"io"

	"leveldb-golang/leveldb/util"
)

var _typeCrc = make([]uint32, kMaxRecordType+1, kMaxRecordType+1)

func init() {
	for i := uint32(0); i <= kMaxRecordType; i++ {
		_typeCrc[i] = util.Crc32Value([]byte{byte(i)})
	}
}

type WriteableFile interface {
	io.Writer

	Flush() error
}

type LogWriter struct {
	dest        WriteableFile
	blockOffset uint32
}

func NewLogWriter(dest WriteableFile) *LogWriter {
	return &LogWriter{
		dest: dest,
	}
}

func NewLogWriterWithInitialOffset(dest WriteableFile, offset uint32) *LogWriter {
	return &LogWriter{
		dest:        dest,
		blockOffset: offset,
	}
}

func (logWriter *LogWriter) AddRecord(slice Slice) *util.LevelDbError {
	startIdx, totalLength := uint32(0), uint32(len(slice))
	start, end := true, false

	// Fragment the record if necessary and emit it.  Note that if slice
	// is empty, we still want to iterate once to emit a single
	// zero-length record
	for {
		leftover := kBlockSize - logWriter.blockOffset // 计算当前block剩余多少byte
		if leftover < kHeaderSize {
			if leftover > 0 {
				if _, err := logWriter.dest.Write(make([]byte, leftover, leftover)); err != nil {
					return util.NewLevelDbError(util.ErrWriteFileFailed, err.Error())
				}
			}
			logWriter.blockOffset = 0
			leftover = kBlockSize
		}

		avail := leftover - kHeaderSize
		fragmentLength := avail
		if startIdx+fragmentLength >= totalLength {
			fragmentLength = totalLength - startIdx
			end = true
		}

		var kType KType
		if start && end {
			kType = kFullType
		} else if start {
			kType = kFirstType
		} else if end {
			kType = kLastType
		} else {
			kType = kMiddleType
		}

		if err := logWriter.EmitPhysicalRecord(kType, slice[startIdx:startIdx+fragmentLength]); err != nil {
			return err
		}

		startIdx += fragmentLength
		start = false
		if startIdx >= totalLength {
			break
		}
	}

	return nil
}

func (logWriter *LogWriter) EmitPhysicalRecord(kType KType, data []byte) *util.LevelDbError {
	header := make([]byte, kHeaderSize, kHeaderSize)

	length := len(data)
	header[4] = uint8(length & 0xff)
	header[5] = uint8(length >> 8)
	header[6] = uint8(kType)

	crc := util.Crc32ValueWithInitial(_typeCrc[kType], data)
	util.EncodeFixedUint32(header, crc)

	if _, err := logWriter.dest.Write(header); err != nil {
		return util.NewLevelDbError(util.ErrWriteFileFailed, err.Error())
	}
	if _, err := logWriter.dest.Write(data); err != nil {
		return util.NewLevelDbError(util.ErrWriteFileFailed, err.Error())
	}

	if err := logWriter.dest.Flush(); err != nil {
		return util.NewLevelDbError(util.ErrFlushFileFailed, err.Error())
	}

	logWriter.blockOffset += kHeaderSize + uint32(length)

	return nil
}
