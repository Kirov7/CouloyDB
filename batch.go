package CouloyDB

import (
	"encoding/binary"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"sync"
)

// WriteBatch Atomic operation writeBatch
type WriteBatch struct {
	options      WriteBatchOptions
	mu           *sync.Mutex
	db           *DB
	pendingWrite map[string]*data.LogRecord // Temporary storage data
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:      opts,
		mu:           new(sync.Mutex),
		db:           db,
		pendingWrite: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return public.ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// Temporarily to deposit logRecord to memory
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrite[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Del(key []byte) error {
	if len(key) == 0 {
		return public.ErrKeyIsEmpty
	}
	wb.mu.Lock()
	defer wb.mu.Unlock()

	recordPos := wb.db.index.getStrIndex().Get(key)
	if recordPos == nil {
		if wb.pendingWrite[string(key)] != nil {
			delete(wb.pendingWrite, string(key))
		}
		return nil
	}

	// Temporarily to deposit logRecord to memory
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrite[string(key)] = logRecord

	return nil
}

func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrite) == 0 {
		return nil
	}

	// get the latest txId
	txId := wb.db.GetTxId()

	positions := make(map[string]*data.LogPos)

	// do appendLogRecord and append it in temp slice
	for _, record := range wb.pendingWrite {
		recordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   encodeKeyWithTxId(record.Key, txId),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		positions[string(record.Key)] = recordPos
	}

	//wb.db.strIndex.Put(record.Key, recordPos)
	// write fin mark
	finishRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(public.TX_COMMIT_KEY, txId),
		Type: data.LogRecordTxnCommit,
	}
	if _, err := wb.db.appendLogRecord(finishRecord); err != nil {
		return err
	}

	if wb.options.SyncWrites {
		if err := wb.db.Sync(); err != nil {
			return err
		}
	}

	// update mem memTable
	for _, record := range wb.pendingWrite {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.getStrIndex().Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.getStrIndex().Del(record.Key)
		}
	}

	// concat pendingWrite
	wb.pendingWrite = make(map[string]*data.LogRecord)
	return nil
}

func encodeKeyWithTxId(key []byte, txId int64) []byte {
	txBin := make([]byte, binary.MaxVarintLen64)
	lenTxBin := binary.PutVarint(txBin[:], txId)
	encKey := make([]byte, lenTxBin+len(key))
	copy(encKey[:lenTxBin], txBin[:lenTxBin])
	copy(encKey[lenTxBin:], key)
	return encKey
}

func deleteInSlice[T comparable](a []T, elem T) []T {
	j := 0
	for _, v := range a {
		if v != elem {
			a[j] = v
			j++
		}
	}
	return a[:j]
}
