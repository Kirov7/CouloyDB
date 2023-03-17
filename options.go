package CouloyDB

import "github.com/Kirov7/CouloyDB/meta"

type Options struct {
	DirPath      string
	DataFileSize int64
	IndexerType  meta.IndexType
	SyncWrites   bool
}
