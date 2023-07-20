package CouloyDB

import (
	"encoding/binary"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
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

func (txn *Txn) getListMeta(key []byte) ([]byte, error) {
	if pw, ok := txn.listMetaPendingWrites[string(key)]; ok {
		v, err := txn.db.getValueByPos(pw.LogPos)
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	if logPos := txn.db.index.getListMetaIndex().Get(key); logPos != nil {
		v, err := txn.db.getValueByPos(logPos)
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	return encodeListMeta(math.MaxFloat64/2, math.MaxFloat64/2), nil
}

func (txn *Txn) push(key []byte, values [][]byte, left bool) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	if _, ok := txn.listDataPendingWrites[string(key)]; !ok {
		txn.listDataPendingWrites[string(key)] = make(map[float64]*pendingWrite)
	}

	listMeta, err := txn.getListMeta(key)
	if err != nil {
		return err
	}
	// get the head and tail seq of the current List
	headSeq, tailSeq := decodeListMeta(listMeta)
	curSeq, prevSeq, nextSeq := txn.allocPushSeq(headSeq, tailSeq, left), float64(0), math.MaxFloat64

	var pw *pendingWrite
	var logPos *data.LogPos

	if left {
		pw = txn.listDataPendingWrites[string(key)][headSeq]
	} else {
		pw = txn.listDataPendingWrites[string(key)][tailSeq]
	}

	if pw != nil && pw.typ != data.LogRecordDeleted {
		logPos = pw.LogPos
	} else if listDataIndex, ok := txn.db.index.getListDataIndex(string(key)); ok {
		if left {
			logPos = listDataIndex.Get(listMeta[:9])
		} else {
			logPos = listDataIndex.Get(listMeta[9:])
		}
	}

	if logPos != nil {
		logRecord, err := txn.db.getLogRecordByPos(logPos)
		if err != nil {
			return err
		}

		realKey, _ := parseLogRecordKey(logRecord.Key)
		decodedKey, seq, prev, next := decodeListKey(realKey)

		var encodedKey []byte
		if left {
			encodedKey = encodeListKey(seq, curSeq, next, decodedKey)
			nextSeq = seq
		} else {
			encodedKey = encodeListKey(seq, prev, curSeq, decodedKey)
			prevSeq = seq
		}

		listDataLogRecord := &data.LogRecord{
			Key:    encodeKeyWithTxId(encodedKey, txn.startTs),
			Value:  logRecord.Value,
			Type:   data.LogRecordNormal,
			DSType: data.List,
		}

		logPos, err = txn.db.appendLogRecordWithLock(listDataLogRecord)
		if err != nil {
			return err
		}

		txn.listDataPendingWrites[string(key)][seq] = &pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}
	}

	for _, value := range values {
		encodedListKey := encodeListKey(curSeq, prevSeq, nextSeq, key)

		listDataLogRecord := &data.LogRecord{
			Key:    encodeKeyWithTxId(encodedListKey, txn.startTs),
			Value:  value,
			Type:   data.LogRecordNormal,
			DSType: data.List,
		}

		logPos, err = txn.db.appendLogRecord(listDataLogRecord)
		if err != nil {
			return err
		}

		txn.listDataPendingWrites[string(key)][curSeq] = &pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}

		if left {
			headSeq = curSeq
			nextSeq = curSeq
		} else {
			tailSeq = curSeq
			prevSeq = curSeq
		}
		curSeq = txn.allocPushSeq(headSeq, tailSeq, left)
	}

	listMetaLogRecord := &data.LogRecord{
		Key:    encodeKeyWithTxId(key, txn.startTs),
		Value:  encodeListMeta(headSeq, tailSeq),
		Type:   data.LogRecordNormal,
		DSType: data.ListMeta,
	}

	logPos, err = txn.db.appendLogRecordWithLock(listMetaLogRecord)
	if err != nil {
		return err
	}

	txn.listMetaPendingWrites[string(key)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}

	return nil
}

func (txn *Txn) allocPushSeq(headSeq, tailSeq float64, left bool) float64 {
	var seq float64
	if left {
		seq = ((headSeq - 1) + headSeq) / 2
	} else {
		seq = ((tailSeq + 1) + tailSeq) / 2
	}
	return seq
}

func encodeListMeta(headSeq, tailSeq float64) []byte {
	buf := make([]byte, binary.MaxVarintLen64*2)
	var index int
	index += binary.PutUvarint(buf[index:], math.Float64bits(headSeq))
	index += binary.PutUvarint(buf[index:], math.Float64bits(tailSeq))
	return buf
}

func decodeListMeta(v []byte) (float64, float64) {
	var index int
	headSeq, i := binary.Uvarint(v[index:])
	index += i
	tailSeq, i := binary.Uvarint(v[index:])
	return math.Float64frombits(headSeq), math.Float64frombits(tailSeq)
}

func encodeListKey(seq, prevSeq, nextSeq float64, value []byte) []byte {
	header := make([]byte, binary.MaxVarintLen64*3)
	var index int
	index += binary.PutUvarint(header[index:], math.Float64bits(seq))
	index += binary.PutUvarint(header[index:], math.Float64bits(prevSeq))
	index += binary.PutUvarint(header[index:], math.Float64bits(nextSeq))
	buf := make([]byte, index+len(value))
	copy(buf[:index], header[:index])
	copy(buf[index:], value)
	return buf
}

func decodeListKey(value []byte) ([]byte, float64, float64, float64) {
	var index int
	seq, i := binary.Uvarint(value[index:])
	index += i
	prevSeq, i := binary.Uvarint(value[index:])
	index += i
	nextSeq, i := binary.Uvarint(value[index:])
	index += i
	return value[index:], math.Float64frombits(seq),
		math.Float64frombits(prevSeq), math.Float64frombits(nextSeq)
}
