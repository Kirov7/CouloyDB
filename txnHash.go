package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
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

	logRecord := &data.LogRecord{
		Key:      encodeKeyWithTxId(encodeFieldKey(key, field), txn.startTs),
		Value:    value,
		Type:     data.LogRecordNormal,
		DataType: data.Hash,
	}

	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	if _, ok := txn.hashPendingWrites[string(key)]; !ok {
		txn.hashPendingWrites[string(key)] = make(map[string]*pendingWrite)
	}
	txn.hashPendingWrites[string(key)][string(field)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
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

	if idx, ok := txn.db.index.getHashIndex(string(key)); ok {
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

	if pw, ok := txn.hashPendingWrites[string(key)][string(field)]; ok {
		if pw.typ == data.LogRecordDeleted {
			return public.ErrKeyNotFound
		}
	} else {
		if idx, ok := txn.db.index.getHashIndex(string(key)); !ok {
			return public.ErrKeyNotFound
		} else {
			if pos := idx.Get(field); pos == nil {
				return public.ErrKeyNotFound
			}
		}
	}

	logRecord := &data.LogRecord{
		Key:      encodeKeyWithTxId(encodeFieldKey(key, field), txn.startTs),
		Type:     data.LogRecordDeleted,
		DataType: data.Hash,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	txn.hashPendingWrites[string(key)][string(field)] = &pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	return nil
}

func (txn *Txn) HExist(key, field []byte) bool {
	if pw, ok := txn.hashPendingWrites[string(key)][string(field)]; ok {
		if pw.typ != data.LogRecordDeleted {
			return true
		}
		return false
	}

	if idx, ok := txn.db.index.getHashIndex(string(key)); ok {
		if pos := idx.Get(field); pos != nil {
			return true
		}
	}
	return true
}

func (txn *Txn) HGetAll(key []byte) ([][]byte, [][]byte, error) {
	fields, values := make([][]byte, 0), make([][]byte, 0)

	for field, pw := range txn.hashPendingWrites[string(key)] {
		if pw.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pw.LogPos)
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, []byte(field))
			values = append(values, v)
		}
	}

	if hash, ok := txn.db.index.getHashIndex(string(key)); ok {
		iterator := hash.Iterator(false)

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			if _, ok := txn.hashPendingWrites[string(key)][string(iterator.Key())]; ok {
				continue
			}

			v, err := txn.db.getValueByPos(iterator.Value())
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, iterator.Key())
			values = append(values, v)
		}
	}

	return fields, values, nil
}

func (txn *Txn) HMGet(key []byte, fields [][]byte) ([][]byte, error) {
	values := make([][]byte, len(fields))

	for i, field := range fields {
		value, err := txn.HGet(key, field)
		if err != nil {
			if err == public.ErrKeyNotFound {
				values[i] = nil
				continue
			}
			return nil, err
		}
		values[i] = value
	}

	return values, nil
}

func (txn *Txn) HMSet(key []byte, args [][]byte) error {
	if len(args)%2 != 0 {
		return public.ErrTxnArgsWrong
	}

	for i := 0; i < len(args); i += 2 {
		err := txn.HSet(key, args[i], args[i+1])
		if err != nil {
			return err
		}
	}

	return nil
}

func (txn *Txn) HKeys(key []byte) ([][]byte, error) {
	fields := make([][]byte, 0)
	for field, pw := range txn.hashPendingWrites[string(key)] {
		if pw.typ != data.LogRecordDeleted {
			fields = append(fields, []byte(field))
		}
	}

	if hash, ok := txn.db.index.getHashIndex(string(key)); ok {
		iterator := hash.Iterator(false)
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			if _, ok := txn.hashPendingWrites[string(key)][string(iterator.Key())]; ok {
				continue
			}
			fields = append(fields, iterator.Key())
		}
	}
	if len(fields) == 0 {
		return [][]byte{}, public.ErrKeyNotFound
	}
	return fields, nil
}

func (txn *Txn) HValues(key []byte) ([][]byte, error) {
	values := make([][]byte, 0)
	for _, pw := range txn.hashPendingWrites[string(key)] {
		if pw.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pw.LogPos)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
	}
	if hash, ok := txn.db.index.getHashIndex(string(key)); ok {
		iterator := hash.Iterator(false)
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			if _, ok := txn.hashPendingWrites[string(key)][string(iterator.Key())]; ok {
				continue
			}
			v, err := txn.db.getValueByPos(iterator.Value())
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return [][]byte{}, public.ErrKeyNotFound
	}
	return values, nil
}

func (txn *Txn) HStrLen(key, field []byte) (int64, error) {
	v, err := txn.HGet(key, field)
	if err != nil {
		return -1, public.ErrKeyNotFound
	}
	return int64(len(string(v))), nil
}

func (txn *Txn) HLen(key []byte) (int64, error) {
	fields, err := txn.HKeys(key)
	if err != nil {
		return -1, err
	}
	return int64(len(fields)), nil
}

func encodeFieldKey(key, field []byte) []byte {
	return bytex.EncodeByteSlices(key, field)
}

func decodeFieldKey(key []byte) ([]byte, []byte) {
	return bytex.DecodeByteSlices(key)
}
