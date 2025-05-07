package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"leveldb-golang/leveldb/util"
)

var (
	initialOffsetRecordSizes = []uint32{
		10000,
		10000,
		2*kBlockSize - 1000,
		1,
		13716,
		kBlockSize - kHeaderSize,
	}
	initialOffsetLastRecordOffsets = []uint32{
		0,
		kHeaderSize + 10000,
		2 * (kHeaderSize + 10000),
		2*(kHeaderSize+10000) + (2*kBlockSize - 1000) + 3*kHeaderSize,
		2*(kHeaderSize+10000) + (2*kBlockSize - 1000) + 3*kHeaderSize + kHeaderSize + 1,
		3 * kBlockSize,
	}
)

type StringDest struct {
	data []byte
}

func NewStringDest() *StringDest {
	return &StringDest{
		data: make([]byte, 0),
	}
}

func (sd *StringDest) Write(p []byte) (n int, err error) {
	sd.data = append(sd.data, p...)
	return len(p), nil
}

func (sd *StringDest) Flush() error {
	return nil
}

func (sd *StringDest) Len() int {
	return len(sd.data)
}

func (sd *StringDest) Data() []byte {
	return sd.data
}

func (sd *StringDest) IncrementByte(offset int, delta uint8) {
	sd.data[offset] += delta
}

func (sd *StringDest) SetByte(offset int, newByte byte) {
	sd.data[offset] = newByte
}

func (sd *StringDest) FixChecksum(headerOffset, len int) {
	newCrc := util.Crc32Value(sd.data[headerOffset+6 : headerOffset+7+len])
	util.EncodeFixedUint32(sd.data[headerOffset:], newCrc)
}

func (sd *StringDest) ShrinkSize(size int) {
	sd.data = sd.data[:len(sd.data)-size]
}

type StringSource struct {
	source *bytes.Reader

	forceError      bool
	returnedPartial bool
}

func NewStringSource() *StringSource {
	return &StringSource{}
}

func (ss *StringSource) SetData(data []byte) {
	ss.source = bytes.NewReader(data)
}

func (ss *StringSource) Read(p []byte) (n int, err error) {
	if ss.returnedPartial {
		panic("must not read after eof/error")
	}
	if ss.forceError {
		ss.returnedPartial = true
		return 0, util.NewLevelDbError(util.ErrReadFileFailed, "read error")
	}

	return ss.source.Read(p)
}

func (ss *StringSource) Seek(offset int64, whence int) (int64, error) {
	return ss.source.Seek(offset, whence)
}

func (ss *StringSource) ForceError() {
	ss.forceError = true
}

type ReportCollector struct {
	droppedBytes uint32
	err          error
}

func NewReportCollector() *ReportCollector {
	return &ReportCollector{}
}

func (rc *ReportCollector) Corruption(n uint32, err error) {
	rc.droppedBytes += n
	rc.err = err
}

func (rc *ReportCollector) DroppedBytes() uint32 {
	return rc.droppedBytes
}

func (rc *ReportCollector) Error() error {
	return rc.err
}

type LogTest struct {
	t *testing.T

	reading  bool
	dest     *StringDest
	writer   *LogWriter
	reporter *ReportCollector
	source   *StringSource
	reader   *LogReader
}

func NewLogTest(t *testing.T) *LogTest {
	dest := NewStringDest()
	source := NewStringSource()
	reporter := NewReportCollector()

	return &LogTest{
		t:        t,
		reading:  false,
		dest:     dest,
		writer:   NewLogWriter(dest),
		reporter: reporter,
		source:   source,
		reader:   NewLogReader(source, reporter, true, 0),
	}
}

func (lt *LogTest) Read() string {
	if !lt.reading {
		lt.reading = true
		lt.source.SetData(lt.dest.Data())
	}

	record, ok := lt.reader.ReadRecord()
	if !ok {
		return "EOF"
	}

	return string(record)
}

func (lt *LogTest) Write(msg string) {
	assert.Falsef(lt.t, lt.reading, "Write after starting to Read")
	err := lt.writer.AddRecord([]byte(msg))
	assert.Nil(lt.t, err)
}

func (lt *LogTest) WrittenBytes() uint32 {
	return uint32(lt.dest.Len())
}

func (lt *LogTest) DroppedBytes() uint32 {
	return lt.reporter.DroppedBytes()
}

func (lt *LogTest) ForceError() {
	lt.source.ForceError()
}

func (lt *LogTest) ReopenForAppend() {
	lt.writer = NewLogWriterWithInitialOffset(lt.dest, lt.WrittenBytes())
}

func (lt *LogTest) WriteInitialOffsetLog() {
	for i := range len(initialOffsetRecordSizes) {
		record := strings.Repeat(string(rune('a'+i)), int(initialOffsetRecordSizes[i]))
		lt.Write(record)
	}
}

func (lt *LogTest) CheckInitialOffsetRecord(initialOffset uint32, expectedRecordOffset int) {
	lt.WriteInitialOffsetLog()
	lt.reading = true
	lt.source.SetData(lt.dest.Data())
	lt.reader = NewLogReader(lt.source, lt.reporter, true, initialOffset)

	assert.Less(lt.t, expectedRecordOffset, len(initialOffsetRecordSizes))
	for i := expectedRecordOffset; i < len(initialOffsetRecordSizes); i++ {
		slice, ok := lt.reader.ReadRecord()
		assert.True(lt.t, ok)
		assert.Equal(lt.t, initialOffsetRecordSizes[i], uint32(len(slice)))
		assert.Equal(lt.t, initialOffsetLastRecordOffsets[i], lt.reader.LastRecordOffset())
		expectedRecord := strings.Repeat(string(rune('a'+i)), int(initialOffsetRecordSizes[i]))
		assert.Equal(lt.t, expectedRecord, string(slice))
	}
}

func (lt *LogTest) CheckOffsetPastEndReturnsNoRecords(offsetPastEnd uint32) {
	lt.WriteInitialOffsetLog()
	lt.reading = true
	lt.source.SetData(lt.dest.Data())
	lt.reader = NewLogReader(lt.source, lt.reporter, true, lt.WrittenBytes()+offsetPastEnd)
	slice, ok := lt.reader.ReadRecord()
	assert.False(lt.t, ok)
	assert.Nil(lt.t, slice)
}

func TestEmpty(t *testing.T) {
	lt := NewLogTest(t)
	assert.Equal(t, "EOF", lt.Read())
}

func TestReadWrite(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.Write("bar")
	lt.Write("")
	lt.Write("xxxx")
	assert.Equal(t, "foo", lt.Read())
	assert.Equal(t, "bar", lt.Read())
	assert.Equal(t, "", lt.Read())
	assert.Equal(t, "xxxx", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestManyBlocks(t *testing.T) {
	lt := NewLogTest(t)
	for i := 0; i < 100000; i++ {
		lt.Write(NumberToString(i))
	}
	for i := 0; i < 100000; i++ {
		assert.Equal(t, NumberToString(i), lt.Read())
	}
	assert.Equal(t, "EOF", lt.Read())
}

func TestFragmentation(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("small")
	lt.Write(BigString("medium", 50000))
	lt.Write(BigString("large", 100000))
	assert.Equal(t, "small", lt.Read())
	assert.Equal(t, BigString("medium", 50000), lt.Read())
	assert.Equal(t, BigString("large", 100000), lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestMarginalTrailer(t *testing.T) {
	// Make a trailer that is exactly the same length as an empty record.
	lt := NewLogTest(t)
	n := kBlockSize - 2*kHeaderSize
	lt.Write(BigString("foo", int(n)))
	assert.Equal(t, kBlockSize-kHeaderSize, lt.WrittenBytes())
	lt.Write("")
	lt.Write("bar")
	assert.Equal(t, BigString("foo", int(n)), lt.Read())
	assert.Equal(t, "", lt.Read())
	assert.Equal(t, "bar", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestMarginalTrailer2(t *testing.T) {
	// Make a trailer that is exactly the same length as an empty record.
	lt := NewLogTest(t)
	n := kBlockSize - 2*kHeaderSize
	lt.Write(BigString("foo", int(n)))
	assert.Equal(t, kBlockSize-kHeaderSize, lt.WrittenBytes())
	lt.Write("bar")
	assert.Equal(t, BigString("foo", int(n)), lt.Read())
	assert.Equal(t, "bar", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
}

func TestShortTrailer(t *testing.T) {
	lt := NewLogTest(t)
	n := kBlockSize - 2*kHeaderSize + 4
	lt.Write(BigString("foo", int(n)))
	assert.Equal(t, kBlockSize-kHeaderSize+4, lt.WrittenBytes())
	lt.Write("")
	lt.Write("bar")
	assert.Equal(t, BigString("foo", int(n)), lt.Read())
	assert.Equal(t, "", lt.Read())
	assert.Equal(t, "bar", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestAlignedEof(t *testing.T) {
	lt := NewLogTest(t)
	n := kBlockSize - 2*kHeaderSize + 4
	lt.Write(BigString("foo", int(n)))
	assert.Equal(t, kBlockSize-kHeaderSize+4, lt.WrittenBytes())
	assert.Equal(t, BigString("foo", int(n)), lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestOpenForAppend(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("hello")
	lt.ReopenForAppend()
	lt.Write("world")
	assert.Equal(t, "hello", lt.Read())
	assert.Equal(t, "world", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
}

func TestRandomRead(t *testing.T) {
	n, seed := 500, int64(301)
	lt := NewLogTest(t)

	rnd := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		lt.Write(NumberToString(rnd.Int()))
	}

	rnd2 := rand.New(rand.NewSource(seed))
	for i := 0; i < n; i++ {
		assert.Equal(t, NumberToString(rnd2.Int()), lt.Read())
	}

	assert.Equal(t, "EOF", lt.Read())
}

func TestReadError(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.ForceError()
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, kBlockSize, lt.DroppedBytes())
	assert.Equal(t, util.ErrReadFileFailed, util.GetErrorNo(lt.reporter.Error()))
}

func TestBadRecordType(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	// Type is stored in header[6]
	lt.dest.IncrementByte(6, 100)
	lt.dest.FixChecksum(0, 3)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(3), lt.DroppedBytes())
	assert.Equal(t, util.ErrUnknownRecordType, util.GetErrorNo(lt.reporter.Error()))
}

func TestTruncatedTrailingRecordIsIgnored(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.dest.ShrinkSize(4) // Drop all payload as well as a header byte
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
}

func TestBadLength(t *testing.T) {
	lt := NewLogTest(t)
	kPayloadSize := kBlockSize - kHeaderSize
	lt.Write(BigString("bar", int(kPayloadSize)))
	lt.Write("foo")
	lt.dest.IncrementByte(4, 1)
	assert.Equal(t, "foo", lt.Read())
	assert.Equal(t, kBlockSize, lt.DroppedBytes())
	assert.Equal(t, util.ErrBadRecordLength, util.GetErrorNo(lt.reporter.Error()))
}

func TestBadLengthAtEndIsIgnored(t *testing.T) {
	// 模拟最后一条消息没写完，writer 就异常退出了。此时应该丢掉最后一条不完整的数据
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.dest.ShrinkSize(1)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
}

func TestChecksumMismatch(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.dest.IncrementByte(0, 10)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(10), lt.DroppedBytes())
	assert.Equal(t, util.ErrCheckCrcFailed, util.GetErrorNo(lt.reporter.Error()))
}

func TestUnexpectedMiddleType(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.dest.SetByte(6, byte(kMiddleType))
	lt.dest.FixChecksum(0, 3)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(3), lt.DroppedBytes())
	assert.Equal(t, util.ErrMissingStart, util.GetErrorNo(lt.reporter.Error()))
}

func TestUnexpectedLastType(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.dest.SetByte(6, byte(kLastType))
	lt.dest.FixChecksum(0, 3)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(3), lt.DroppedBytes())
	assert.Equal(t, util.ErrMissingStart, util.GetErrorNo(lt.reporter.Error()))
}

func TestUnexpectedFullType(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.Write("bar")
	lt.dest.SetByte(6, byte(kFirstType))
	lt.dest.FixChecksum(0, 3)
	assert.Equal(t, "bar", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(3), lt.DroppedBytes())
	assert.Equal(t, util.ErrPartialRecordWithoutEnd, util.GetErrorNo(lt.reporter.Error()))
}

func TestUnexpectedFirstType(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write("foo")
	lt.Write(BigString("bar", 100000))
	lt.dest.SetByte(6, byte(kFirstType))
	lt.dest.FixChecksum(0, 3)
	assert.Equal(t, BigString("bar", 100000), lt.Read())
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(3), lt.DroppedBytes())
	assert.Equal(t, util.ErrPartialRecordWithoutEnd, util.GetErrorNo(lt.reporter.Error()))
}

func TestMissingLastIsIgnored(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write(BigString("bar", int(kBlockSize)))
	lt.dest.ShrinkSize(14)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
}

func TestPartialLastIsIgnored(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write(BigString("bar", int(kBlockSize)))
	lt.dest.ShrinkSize(1)
	assert.Equal(t, "EOF", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
}

func TestSkipIntoMultiRecord(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write(BigString("foo", int(3*kBlockSize)))
	lt.Write("correct")

	lt.reader = NewLogReader(lt.source, lt.reporter, true, kBlockSize)
	assert.Equal(t, "correct", lt.Read())
	assert.Equal(t, uint32(0), lt.DroppedBytes())
	assert.Equal(t, util.ErrOk, util.GetErrorNo(lt.reporter.Error()))
	assert.Equal(t, "EOF", lt.Read())
}

func TestErrorJoinsRecords(t *testing.T) {
	lt := NewLogTest(t)
	lt.Write(BigString("foo", int(kBlockSize)))
	lt.Write(BigString("bar", int(kBlockSize)))
	lt.Write("correct")

	for offset := kBlockSize; offset < 2*kBlockSize; offset++ {
		lt.dest.SetByte(int(offset), 'x')
	}

	assert.Equal(t, "correct", lt.Read())
	assert.Equal(t, "EOF", lt.Read())
	assert.Less(t, lt.DroppedBytes(), 2*kBlockSize+100)
	assert.Greater(t, lt.DroppedBytes(), 2*kBlockSize)
}

func TestReadStart(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(0, 0)
}

func TestReadSecondOneOff(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(1, 1)
}

func TestReadSecondTenThousand(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(10000, 1)
}

func TestReadSecondStart(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(10007, 1)
}

func TestReadThirdOneOff(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(10008, 2)
}

func TestReadThirdStart(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(20014, 2)
}

func TestReadFourthOneOff(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(20015, 3)
}

func TestReadFourthFirstBlockTrailer(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(kBlockSize-4, 3)
}

func TestReadFourthMiddleBlock(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(kBlockSize+1, 3)
}

func TestReadFourthLastBlock(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(2*kBlockSize+1, 3)
}

func TestReadFourthStart(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(2*(kHeaderSize+1000)+(2*kBlockSize-1000)+3*kHeaderSize, 3)
}

func TestReadInitialOffsetIntoBlockPadding(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckInitialOffsetRecord(3*kBlockSize-3, 5)
}

func TestReadEnd(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckOffsetPastEndReturnsNoRecords(0)
}

func TestReadPastEnd(t *testing.T) {
	lt := NewLogTest(t)
	lt.CheckOffsetPastEndReturnsNoRecords(5)
}

func NumberToString(n int) string {
	return fmt.Sprintf("%d.", n)
}

func BigString(partialString string, n int) string {
	data := make([]byte, 0, n+len(partialString))
	partial := []byte(partialString)
	for len(data) < n {
		data = append(data, partial...)
	}
	data = data[:n]
	return string(data)
}
