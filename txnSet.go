package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/bytex"
	"github.com/Kirov7/CouloyDB/public/utils/consistent"
)

func (txn *Txn) SAdd(key []byte, members ...[]byte) error {
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
			Key:      encodeKeyWithTxId(encodeMemberKey(key, member), txn.startTs),
			Value:    member,
			Type:     data.LogRecordNormal,
			DataType: data.Set,
		}

		pos, err := txn.db.appendLogRecordWithLock(logRecord)
		if err != nil {
			return err
		}

		if _, ok := txn.setPendingWrites[string(key)]; !ok {
			txn.setPendingWrites[string(key)] = make(map[string]*pendingWrite)
		}

		hashKey := hashMemberKey(key, member)
		txn.setPendingWrites[string(key)][string(hashKey)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
	}
	return nil
}

func (txn *Txn) SRem(key []byte, members ...[]byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}

	for _, member := range members {
		hashKey := hashMemberKey(key, member)
		if pw, ok := txn.setPendingWrites[string(key)][string(member)]; ok {
			if pw.typ == data.LogRecordDeleted {
				return public.ErrKeyNotFound
			}
		} else {
			if idx, ok := txn.db.index.getSetIndex(string(key)); !ok {
				return public.ErrKeyNotFound
			} else {
				if pos := idx.Get(hashKey); pos == nil {
					return public.ErrKeyNotFound
				}
			}
		}

		logRecord := &data.LogRecord{
			Key:      encodeKeyWithTxId(encodeMemberKey(key, member), txn.startTs),
			Type:     data.LogRecordDeleted,
			DataType: data.Set,
		}

		pos, err := txn.db.appendLogRecordWithLock(logRecord)
		if err != nil {
			return err
		}

		if _, ok := txn.setPendingWrites[string(key)]; !ok {
			txn.setPendingWrites[string(key)] = make(map[string]*pendingWrite)
		}

		txn.setPendingWrites[string(key)][string(hashKey)] = &pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	}

	return nil
}

func (txn *Txn) SMembers(key []byte) ([][]byte, error) {
	members := make([][]byte, 0)
	for _, pw := range txn.setPendingWrites[string(key)] {
		if pw.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pw.LogPos)
			if err != nil {
				return nil, err
			}
			members = append(members, v)
		}
	}

	if setIdx, ok := txn.db.index.getSetIndex(string(key)); ok {
		iterator := setIdx.Iterator(false)

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			v, err := txn.db.getValueByPos(iterator.Value())
			if err != nil {
				return nil, err
			}
			members = append(members, v)

		}
	}
	return members, nil
}

func (txn *Txn) SCard(key []byte) (int64, error) {
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

func hashMemberKey(key, member []byte) []byte {
	c := consistent.New()
	encodeKey := encodeMemberKey(key, member)
	return c.HashKey(string(encodeKey))
}

func encodeMemberKey(key, member []byte) []byte {
	return bytex.EncodeByteSlices(key, member)
}

func decodeMemberKey(data []byte) ([]byte, []byte) {
	return bytex.DecodeByteSlices(data)
}
