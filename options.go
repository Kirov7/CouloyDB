package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/meta"
)

type Options struct {
	DirPath      string
	DataFileSize int64
	IndexerType  meta.MemTableType
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
