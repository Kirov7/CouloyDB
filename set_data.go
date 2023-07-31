package CouloyDB

import (
	"time"

	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/ds"
)

func (db *DB) SADD(key, value []byte) error {
	return db.sADD(key, value, 0)
}

func (db *DB) sADD(key, value []byte, duration time.Duration) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if err := checkKey(value); err != nil {
		return err
	}

	db.getIndexLockByType(data.Hash).Lock()
	defer db.getIndexLockByType(data.Hash).Unlock()

	var expiration int64
	if duration != 0 {
		expiration = time.Now().Add(duration).UnixNano()
		db.ttl.add(ds.NewJob(string(key), time.Unix(0, expiration)))
	} else {
		// If it is a key without an expiration time set
		// you may need to remove the previously set expiration time
		db.ttl.del(string(key))
	}

	logRecord := &data.LogRecord{
		Key:        encodeKeyWithTxId(key, public.NO_TX_ID),
		Value:      value,
		Type:       data.LogRecordNormal,
		DSType:     data.Hash,
		Expiration: expiration,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	db.Notify(string(key), value, PutEvent)

	var hashIndex meta.MemTable
	hashIndex, ok := db.index.getHashIndex(string(key))
	if !ok {
		hashIndex = meta.NewHashMap()
		db.index.setHashIndex(string(key), hashIndex)
	}

	hashIndex.Put(value, pos)
	return nil
}

func (db *DB) SMEMBERS(key []byte) ([][]byte, error) {
	if len(key) == 0 {
		return nil, public.ErrKeyIsEmpty
	}

	db.getIndexLockByType(data.Hash).RLock()
	defer db.getIndexLockByType(data.Hash).RUnlock()

	if db.ttl.isExpired(string(key)) {
		// if the key is expired, just return and don't delete the key now
		return nil, public.ErrKeyNotFound
	}

	values := db.ListMembers(key)

	return values, nil
}

func (db *DB) ListMembers(key []byte) [][]byte {
	db.getIndexLockByType(data.Hash).RLock()
	defer db.getIndexLockByType(data.Hash).RUnlock()
	hashIndex, ok := db.index.getHashIndex(string(key))
	if !ok {
		return nil
	}
	values := make([][]byte, hashIndex.Count())
	iterator := hashIndex.Iterator(false)
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		values[idx] = iterator.Key()
		idx++
	}
	return values
}

func (db *DB) SCARD(key []byte) (int, error) {
	if len(key) == 0 {
		return 0, public.ErrKeyIsEmpty
	}

	db.getIndexLockByType(data.Hash).RLock()
	defer db.getIndexLockByType(data.Hash).RUnlock()

	hashIndex, ok := db.index.getHashIndex(string(key))
	if !ok {
		return 0, public.ErrKeyNotFound
	}

	count := hashIndex.Count()

	return count, nil
}
