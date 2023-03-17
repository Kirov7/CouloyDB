package driver

import "os"

// FileIO Standard file IO
type FileIO struct {
	// System file descriptors
	fd *os.File
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (f *FileIO) Read(bytes []byte, offset int64) (int, error) {
	return f.fd.ReadAt(bytes, offset)
}

func (f *FileIO) Write(bytes []byte) (int, error) {
	return f.fd.Write(bytes)
}

func (f *FileIO) Sync() error {
	return f.fd.Sync()
}

func (f *FileIO) Close() error {
	return f.fd.Close()
}

func (f *FileIO) Size() (int64, error) {
	stat, err := f.fd.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}
