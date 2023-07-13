package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
)

func (txn *Txn) HSet(key, filed, value []byte) error {
	return nil
}

func (txn *Txn) HGet(key, filed []byte) ([]byte, error) {
	if pw, ok := txn.hashPendingWrites[string(key)][string(filed)]; ok {
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
		if pos := idx.Get(key); pos != nil {
			return txn.db.getValueByPos(pos)
		}
	}
	return nil, public.ErrKeyNotFound
}

func (txn *Txn) HDel(key, filed []byte) error {
	return nil
}

func (txn *Txn) HExist(key, filed []byte) bool {
	if pw, ok := txn.hashPendingWrites[string(key)][string(filed)]; ok {
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
