package data

import (
	"errors"
	"fmt"
	"github.com/Kirov7/CouloyDB/driver"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc value, logRecord maybe corrupted")
)

const DataFileNameSuffix = ".kly"
const HintFileName = "hint-index"
const MergeFinishedFileName = "merge-finished"
const TxIDFileName = "merge-finished"

type DataFile struct {
	FileId   uint32
	WriteOff int64
	Writer   driver.IOManager
	Reader   driver.IOManager
}

// OpenDataFile Open new datafile
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	return newDataFile(fileName, fileId)
}

// OpenHintFile Open new datafile
func OpenHintFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, HintFileName)
	return newDataFile(fileName, 0)
}

// OpenMergeFinishedFile Open new datafile
func OpenMergeFinishedFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, MergeFinishedFileName)
	return newDataFile(fileName, 0)
}

// OpenTxIDFile Open new datafile
func OpenTxIDFile(dirPath string) (*DataFile, error) {
	fileName := filepath.Join(dirPath, TxIDFileName)
	return newDataFile(fileName, 0)
}

func GetDataFileName(dirPath string, fileId uint32) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
}

func newDataFile(fileName string, fileId uint32) (*DataFile, error) {
	writer, err := driver.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	reader, err := driver.NewMMap(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:   fileId,
		WriteOff: 0,
		Writer:   writer,
		Reader:   reader,
	}, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.Writer.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// read Header
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	// if read the end of the file
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.RecordType}
	// read the real k-v
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// parsing the key and the value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// check crc
	crc := GetLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, recordSize, nil
}

// WriteHintRecord write the index info to the hint file
func (df *DataFile) WriteHintRecord(key []byte, pos *LogPos) error {
	record := &LogRecord{
		Key:   key,
		Value: EncodeLogRecordPos(pos),
	}
	encRecord, _ := EncodeLogRecord(record)
	return df.Write(encRecord)
}

func (df *DataFile) Sync() error {
	return df.Writer.Sync()
}

func (df *DataFile) Close() error {
	return df.Writer.Close()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.Writer.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)

	_, err = df.Reader.Read(b, offset)
	return nil, err
}
