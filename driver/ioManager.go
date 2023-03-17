package driver

const (
	DataFilePerm = 0644
)

type IOManager interface {
	// Read By specifying the location data in a read the file
	Read([]byte, int64) (int, error)

	// Write Writing bytes of data to a file
	Write([]byte) (int, error)

	// Sync Make data persistent
	Sync() error

	//Close Close the driver
	Close() error

	Size() (int64, error)
}

// NewIOManager Init IOManager instance
// attention! the IOManager is only support FileIO
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
