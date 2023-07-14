package CouloyDB

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/meta"
)

type Iterator struct {
	options       IteratorOptions
	IndexIterator meta.Iterator
	db            *DB
}

func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	iterator := db.index.getStrIndex().Iterator(options.Reverse)
	return &Iterator{
		IndexIterator: iterator,
		db:            db,
		options:       options,
	}
}

func (it *Iterator) Rewind() {
	it.IndexIterator.Rewind()
	it.skipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.IndexIterator.Seek(key)
	it.skipToNext()
}

func (it *Iterator) Next() {
	it.IndexIterator.Next()
	it.skipToNext()
}

func (it *Iterator) Valid() bool {
	return it.IndexIterator.Valid()
}

func (it *Iterator) Key() []byte {
	return it.IndexIterator.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.IndexIterator.Value()
	return it.db.getValueByPos(logRecordPos)

}

func (it *Iterator) Close() {
	it.IndexIterator.Close()
}

func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.IndexIterator.Valid(); it.IndexIterator.Next() {
		key := it.IndexIterator.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
