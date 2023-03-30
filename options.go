package CouloyDB

type Options struct {
	DirPath      string
	DataFileSize int64
	IndexerType  IndexType
	SyncWrites   bool
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type WriteBatchOptions struct {
	MaxBatchNum uint32
	SyncWrites  bool
}

type IndexType = int8

const (
	Btree IndexType = iota
)
