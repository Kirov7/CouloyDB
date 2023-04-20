package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFin
)
const (
	// crc type keySize ValueSize
	// 4 + 1 + 5 + 5 = 15
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5
)

type LogRecordHeader struct {
	crc        uint32
	RecordType LogRecordType
	KeySize    uint32
	ValueSize  uint32
}

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogPos The location of the data on the disk
type LogPos struct {
	Fid    uint32
	Offset int64
}

func EncodeLogRecord(log *LogRecord) ([]byte, int64) {
	// init header
	header := make([]byte, maxLogRecordHeaderSize)

	// 5th byte store the Type
	header[4] = log.Type
	var index = 5
	// after the 5th byte the data we store is the key and value with varInt
	index += binary.PutVarint(header[index:], int64(len(log.Key)))
	index += binary.PutVarint(header[index:], int64(len(log.Value)))

	var size = index + len(log.Key) + len(log.Value)
	encBytes := make([]byte, size)
	// copy the header to bytes
	copy(encBytes[:index], header[:index])
	// copy the key to bytes
	copy(encBytes[index:], log.Key)
	// copy the value to bytes
	copy(encBytes[index+len(log.Key):], log.Value)

	// check crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		RecordType: buf[4],
	}

	var index = 5

	// read the real keySize
	keySize, n := binary.Varint(buf[index:])
	header.KeySize = uint32(keySize)
	index += n

	// read the real valueSize
	valueSize, n := binary.Varint(buf[index:])
	header.ValueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

// EncodeLogRecordPos encode the pos info
func EncodeLogRecordPos(pos *LogPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	return buf[:index]
}

func DecodeLogRecordPos(buf []byte) *LogPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, _ := binary.Varint(buf[index:])
	return &LogPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

func GetLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)

	return crc
}

// TxRecord Transaction data temporarily stored in memory
type TxRecord struct {
	Record *LogRecord
	Pos    *LogPos
}
