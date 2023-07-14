package CouloyDB

import (
	"encoding/binary"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
)

func (txn *Txn) HSet(key, field, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	if err := checkKey(field); err != nil {
		return err
	}
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	if _, ok := txn.db.hashIndex[string(key)]; !ok {
		txn.db.hashIndex[string(key)] = meta.NewMemTable(txn.db.options.IndexType)
	}
	logRecord := &data.LogRecord{
		Key:    encodeKeyWithTxId(encodeFiledKey(key, field), txn.startTs),
		Value:  value,
		Type:   data.LogRecordNormal,
		DSType: data.Hash,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if _, ok := txn.hashPendingWrites[string(key)]; !ok {
		txn.hashPendingWrites[string(key)] = make(map[string]pendingWrite)
	}
	txn.hashPendingWrites[string(key)][string(field)] = pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
	return nil
}

func (txn *Txn) HGet(key, field []byte) ([]byte, error) {
	if pw, ok := txn.hashPendingWrites[string(key)][string(field)]; ok {
		if pw.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pw.LogPos)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
		return nil, public.ErrKeyNotFound
	}
	if idx, ok := txn.db.hashIndex[string(key)]; ok {
		if pos := idx.Get(field); pos != nil {
			return txn.db.getValueByPos(pos)
		}
	}
	return nil, public.ErrKeyNotFound
}

func (txn *Txn) HDel(key, field []byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	logRecord := &data.LogRecord{
		Key:    encodeKeyWithTxId(encodeFiledKey(key, field), txn.startTs),
		Type:   data.LogRecordDeleted,
		DSType: data.Hash,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	txn.hashPendingWrites[string(key)][string(field)] = pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	return nil
}

func (txn *Txn) HExist(key, field []byte) bool {
	if pw, ok := txn.hashPendingWrites[string(key)][string(field)]; ok {
		if pw.typ != data.LogRecordDeleted {
			return true
		}
		return false
	}
	if idx, ok := txn.db.hashIndex[string(key)]; ok {
		if pos := idx.Get(key); pos != nil {
			return true
		}
	}
	return true
}

func (txn *Txn) HGetAll(key []byte) ([][]byte, [][]byte, error) {
	keys, values := make([][]byte, 0), make([][]byte, 0)

	hash := txn.db.hashIndex[string(key)]
	iterator := hash.Iterator(false)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		if pw, ok := txn.hashPendingWrites[string(key)][string(iterator.Key())]; ok {
			if pw.typ != data.LogRecordDeleted {
				v, err := txn.db.getValueByPos(pw.LogPos)
				if err != nil {
					return nil, nil, err
				}
				keys = append(keys, iterator.Key())
				values = append(values, v)
			}
			continue
		}

		v, err := txn.db.getValueByPos(iterator.Value())
		if err != nil {
			return nil, nil, err
		}
		keys = append(keys, iterator.Key())
		values = append(values, v)
	}
	return keys, values, nil
}

func encodeFiledKey(key, field []byte) []byte {
	header := make([]byte, binary.MaxVarintLen64*2)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(field)))
	length := len(key) + len(field)
	buf := make([]byte, length+index)
	copy(buf[:index], header[:index])
	copy(buf[index:index+len(key)], key)
	copy(buf[index+len(key):], field)
	return buf
}

func decodeFiledKey(key []byte) ([]byte, []byte) {
	var index int
	keySize, i := binary.Varint(key[index:])
	index += i
	_, i = binary.Varint(key[index:])
	index += i
	sep := index + int(keySize)
	return key[index:sep], key[sep:]
}
