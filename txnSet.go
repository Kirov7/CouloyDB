package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
)

func (txn *Txn) SADD(key []byte, members ...[]byte) error {
	if err := checkKey(key); err != nil {
		return err
	}
	for _, member := range members {
		if err := checkKey(member); err != nil {
			return err
		}

		if txn.readOnly {
			return public.ErrUpdateInReadOnlyTxn
		}

		if _, ok := txn.db.index.getSetIndex(string(key)); !ok {
			txn.db.index.setSetIndex(string(key), meta.NewMemTable(txn.db.options.IndexType))
		}

		logRecord := &data.LogRecord{
			Key:    encodeKeyWithTxId(encodeFieldKey(key, member), txn.startTs),
			Value:  member,
			Type:   data.LogRecordNormal,
			DSType: data.Set,
		}

		pos, err := txn.db.appendLogRecordWithLock(logRecord)
		if err != nil {
			return err
		}

		if _, ok := txn.setPendingWrites[string(key)]; !ok {
			txn.setPendingWrites[string(key)] = make(map[string]pendingWrite)
		}
		txn.setPendingWrites[string(key)][string(member)] = pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
	}
	return nil
}

func (txn *Txn) SREM(key []byte, members ...[]byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}

	for _, member := range members {
		if pw, ok := txn.setPendingWrites[string(key)][string(member)]; ok {
			if pw.typ == data.LogRecordDeleted {
				return public.ErrKeyNotFound
			}
		} else {
			if idx, ok := txn.db.index.getSetIndex(string(key)); !ok {
				return public.ErrKeyNotFound
			} else {
				if pos := idx.Get(member); pos == nil {
					return public.ErrKeyNotFound
				}
			}
		}

		logRecord := &data.LogRecord{
			Key:    encodeKeyWithTxId(encodeFieldKey(key, member), txn.startTs),
			Type:   data.LogRecordDeleted,
			DSType: data.Set,
		}

		pos, err := txn.db.appendLogRecordWithLock(logRecord)
		if err != nil {
			return err
		}

		if _, ok := txn.setPendingWrites[string(key)]; !ok {
			txn.setPendingWrites[string(key)] = make(map[string]pendingWrite)
		}

		txn.setPendingWrites[string(key)][string(member)] = pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	}

	return nil
}

func (txn *Txn) SMEMBERS(key []byte) ([][]byte, error) {
	members := make([][]byte, 0)

	setIdx, ok := txn.db.index.getSetIndex(string(key))
	if !ok {
		return nil, public.ErrKeyNotFound
	}

	iterator := setIdx.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		if pw, ok := txn.setPendingWrites[string(key)][string(iterator.Key())]; ok {
			if pw.typ != data.LogRecordDeleted {
				_, err := txn.db.getValueByPos(pw.LogPos)
				if err != nil {
					return nil, err
				}
				members = append(members, iterator.Key())
			}
			continue
		}

		v, err := txn.db.getValueByPos(iterator.Value())
		if err != nil {
			return nil, err
		}
		members = append(members, v)
	}
	return members, nil
}

func (txn *Txn) SCARD(key []byte) (int64, error) {
	var count int64
	setIdx, ok := txn.db.index.getSetIndex(string(key))
	if !ok {
		return 0, public.ErrKeyNotFound
	}

	iterator := setIdx.Iterator(false)

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		if pw, ok := txn.setPendingWrites[string(key)][string(iterator.Key())]; ok {
			if pw.typ != data.LogRecordDeleted {
				_, err := txn.db.getValueByPos(pw.LogPos)
				if err != nil {
					return 0, err
				}
				count++
			}
			continue
		}

		_, err := txn.db.getValueByPos(iterator.Value())
		if err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}
