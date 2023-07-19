package CouloyDB

import (
	"encoding/binary"
	"github.com/Kirov7/CouloyDB/data"
	"math"
)

func (txn *Txn) LPush(key []byte, values [][]byte) error {
	return txn.push(key, values, true)
}

func (txn *Txn) RPush(key []byte, values [][]byte) error {
	return txn.push(key, values, false)
}

func (txn *Txn) LPop(key []byte) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) RPop(key []byte) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) LLen(key []byte) (int, error) {
	return 0, nil
}

func (txn *Txn) LIndex(key []byte, index int) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) LSet(key []byte, index int, value []byte) error {
	return nil
}

func (txn *Txn) LRem(key []byte, index int) error {
	return nil
}

func (txn *Txn) LRange(key []byte, start, stop int) ([][]byte, error) {
	return nil, nil
}

func (txn *Txn) LTrim(key []byte, start, stop int) error {
	return nil
}

func (txn *Txn) getListMeta(key []byte) (float64, float64, error) {
	var headSeq, tailSeq float64
	if pw, ok := txn.listMetaPendingWrites[string(key)]; ok {
		v, err := txn.db.getValueByPos(pw.LogPos)
		if err != nil {
			return 0, 0, err
		}
		headSeq, tailSeq = decodeListMeta(v)
		return headSeq, tailSeq, nil
	}

	if logPos := txn.db.index.getListMetaIndex().Get(key); logPos != nil {
		v, err := txn.db.getValueByPos(logPos)
		if err != nil {
			return 0, 0, err
		}
		headSeq, tailSeq = decodeListMeta(v)
		return headSeq, tailSeq, nil
	}

	return math.MaxFloat64 / 2, math.MaxFloat64 / 2, nil
}

func (txn *Txn) push(key []byte, values [][]byte, left bool) error {
	headSeq, tailSeq, err := txn.getListMeta(key)
	if err != nil {
		return err
	}

	for _, value := range values {
		var encodedListKey []byte

		if left {
			prevSeq := headSeq - 1
			headSeq = (headSeq + prevSeq) / 2
			encodedListKey = encodeListKey(headSeq, key)
		} else {
			nextSeq := tailSeq + 1
			tailSeq = (tailSeq + nextSeq) / 2
			encodedListKey = encodeListKey(tailSeq, key)
		}

		listDataLogRecord := &data.LogRecord{
			Key:    encodeKeyWithTxId(encodedListKey, txn.startTs),
			Value:  value,
			Type:   data.LogRecordNormal,
			DSType: data.List,
		}

		logPos, err := txn.db.appendLogRecord(listDataLogRecord)
		if err != nil {
			return err
		}

		if _, ok := txn.listDataPendingWrites[string(key)]; !ok {
			txn.listDataPendingWrites[string(key)] = make(map[float64]pendingWrite)
		}
		txn.listDataPendingWrites[string(key)][headSeq] = pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}
	}

	listMetaLogRecord := &data.LogRecord{
		Key:    encodeKeyWithTxId(key, txn.startTs),
		Value:  encodeListMeta(headSeq, tailSeq),
		Type:   data.LogRecordNormal,
		DSType: data.ListMeta,
	}

	logPos, err := txn.db.appendLogRecordWithLock(listMetaLogRecord)
	if err != nil {
		return err
	}

	txn.listMetaPendingWrites[string(key)] = pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}

	return nil
}

func encodeListMeta(headSeq, tailSeq float64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], math.Float64bits(headSeq))
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(tailSeq))
	return buf
}

func decodeListMeta(v []byte) (float64, float64) {
	return math.Float64frombits(binary.BigEndian.Uint64(v[:8])), math.Float64frombits(binary.BigEndian.Uint64(v[8:16]))
}

func encodeListKey(seq float64, key []byte) []byte {
	buf := make([]byte, 8+len(key))
	binary.BigEndian.PutUint64(buf[:8], math.Float64bits(seq))
	copy(buf[8:], key)
	return buf
}

func decodeListKey(key []byte) ([]byte, float64) {
	return key[8:], math.Float64frombits(binary.BigEndian.Uint64(key[:8]))
}
