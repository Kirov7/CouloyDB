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

const DataFileNameSuffix = ".cly"

type DataFile struct {
	FileId    uint32
	WriteOff  int64
	IoManager driver.IOManager
}

// OpenDataFile Open new datafile
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	ioManager, err := driver.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: ioManager,
	}, nil
}

func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
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

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return nil, err
}
