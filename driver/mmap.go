package driver

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMap struct {
	readAt *mmap.ReaderAt
}

func NewMMap(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readAt: readAt}, nil
}

func (m *MMap) Read(bytes []byte, offset int64) (int, error) {
	return m.readAt.ReadAt(bytes, offset)
}

func (m *MMap) Write(bytes []byte) (int, error) {
	panic("mmap does not support Write operations")
}

func (m *MMap) Sync() error {
	panic("mmap does not support Sync operations")
}

func (m *MMap) Close() error {
	return m.readAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readAt.Len()), nil
}
