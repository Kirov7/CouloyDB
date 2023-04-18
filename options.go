package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/meta"
	"os"
)

type Options struct {
	DirPath      string
	DataFileSize int64
	IndexType    meta.MemTableType
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

func DefaultOptions() Options {
	return Options{
		DirPath:      os.TempDir(),
		DataFileSize: 256 * 1024 * 1024, // 256MB
		IndexType:    meta.Btree,
		SyncWrites:   false,
	}
}

func (o *Options) SetDirPath(path string) *Options {
	o.DirPath = path
	return o
}

func (o *Options) SetIndexType(typ meta.MemTableType) *Options {
	o.IndexType = typ
	return o
}

func (o *Options) SetDataFileSizeByte(size int64) *Options {
	o.DataFileSize = size
	return o
}

func (o *Options) SetDataFileSizeKB(size int64) *Options {
	o.DataFileSize = size * 1024
	return o
}

func (o *Options) SetDataFileSizeMB(size int64) *Options {
	o.DataFileSize = size * 1024 * 1024
	return o
}

func (o *Options) SetDataFileSizeGB(size int64) *Options {
	o.DataFileSize = size * 1024 * 1024 * 1024
	return o
}

func (o *Options) SetSyncWrites(sync bool) *Options {
	o.SyncWrites = sync
	return o
}
