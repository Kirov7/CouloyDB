package CouloyDB

import (
	"encoding/binary"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"math/big"
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

func (txn *Txn) getListMeta(key []byte) (*big.Float, *big.Float, error) {
	var headSeq, tailSeq *big.Float
	if pw, ok := txn.listMetaPendingWrites[string(key)]; ok {
		if pw.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pw.LogPos)
			if err != nil {
				return big.NewFloat(0), big.NewFloat(0), err
			}
			headSeq, tailSeq = decodeListMeta(v)
			return headSeq, tailSeq, nil
		}
	} else if logPos := txn.db.index.getListMetaIndex().Get(key); logPos != nil {
		v, err := txn.db.getValueByPos(logPos)
		if err != nil {
			return big.NewFloat(0), big.NewFloat(0), err
		}
		headSeq, tailSeq = decodeListMeta(v)
		return headSeq, tailSeq, nil
	}

	return big.NewFloat(1), big.NewFloat(0), nil
}

func (txn *Txn) push(key []byte, values [][]byte, isLeft bool) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	if _, ok := txn.listDataPendingWrites[string(key)]; !ok {
		txn.listDataPendingWrites[string(key)] = make(map[string]*pendingWrite)
	}

	headSeq, tailSeq, err := txn.getListMeta(key)
	if err != nil {
		return err
	}

	curSeq, prevSeq, nextSeq := new(big.Float), new(big.Float), new(big.Float)

	for _, value := range values {
		curSeq = txn.allocPushSeq(headSeq, tailSeq, isLeft)
		if isLeft {
			nextSeq = headSeq
			prevSeq = prevSeq.Sub(curSeq, big.NewFloat(1))
		} else {
			nextSeq = nextSeq.Add(curSeq, big.NewFloat(1))
			prevSeq = tailSeq
		}

		encodedListKey := encodeListKey(curSeq, prevSeq, nextSeq, key)

		listDataLogRecord := &data.LogRecord{
			Key:      encodeKeyWithTxId(encodedListKey, txn.startTs),
			Value:    value,
			Type:     data.LogRecordNormal,
			DataType: data.List,
		}

		logPos, err := txn.db.appendLogRecord(listDataLogRecord)
		if err != nil {
			return err
		}

		buf, err := curSeq.GobEncode()
		if err != nil {
			return err
		}

		txn.listDataPendingWrites[string(key)][string(buf)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}

		if isLeft {
			headSeq = curSeq
		} else {
			tailSeq = curSeq
		}
	}

	listMetaLogRecord := &data.LogRecord{
		Key:      encodeKeyWithTxId(key, txn.startTs),
		Value:    encodeListMeta(headSeq, tailSeq),
		Type:     data.LogRecordNormal,
		DataType: data.ListMeta,
	}

	logPos, err := txn.db.appendLogRecordWithLock(listMetaLogRecord)
	if err != nil {
		return err
	}

	txn.listMetaPendingWrites[string(key)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: logPos}

	return nil
}

func (txn *Txn) allocPushSeq(headSeq, tailSeq *big.Float, left bool) *big.Float {
	seq := new(big.Float)
	if left {
		seq = seq.Sub(headSeq, big.NewFloat(1))
	} else {
		seq = seq.Add(tailSeq, big.NewFloat(1))
	}
	return seq
}

func (txn *Txn) getLogPosByLeftOrRight(key []byte, left, right *big.Float, isLeft bool) *data.LogPos {
	var pw *pendingWrite
	var logPos *data.LogPos

	leftBuf, _ := left.GobEncode()
	rightBuf, _ := right.GobEncode()

	if isLeft {
		pw = txn.listDataPendingWrites[string(key)][string(leftBuf)]
	} else {
		pw = txn.listDataPendingWrites[string(key)][string(rightBuf)]
	}

	if pw != nil && pw.typ != data.LogRecordDeleted {
		logPos = pw.LogPos
	} else if listDataIndex, ok := txn.db.index.getListDataIndex(string(key)); ok {
		if isLeft {
			logPos = listDataIndex.Get(leftBuf)
		} else {
			logPos = listDataIndex.Get(rightBuf)
		}
	}
	return logPos
}

func encodeListMeta(headSeq, tailSeq *big.Float) []byte {
	headSeqBuf, _ := headSeq.GobEncode()
	tailSeqBuf, _ := tailSeq.GobEncode()
	header := make([]byte, binary.MaxVarintLen64*2)
	var index int
	index += binary.PutVarint(header[index:], int64(len(headSeqBuf)))
	index += binary.PutVarint(header[index:], int64(len(tailSeqBuf)))
	buf := make([]byte, len(headSeqBuf)+len(tailSeqBuf)+index)
	copy(buf[:index], header[:index])
	copy(buf[index:index+len(headSeqBuf)], headSeqBuf)
	copy(buf[index+len(headSeqBuf):], tailSeqBuf)
	return buf
}

func decodeListMeta(listMeta []byte) (*big.Float, *big.Float) {
	var index int
	headSeqLen, i := binary.Varint(listMeta[index:])
	index += i
	_, i = binary.Varint(listMeta[index:])
	index += i
	headSeq, tailSeq := big.NewFloat(0), big.NewFloat(0)
	_ = headSeq.GobDecode(listMeta[index : index+int(headSeqLen)])
	_ = tailSeq.GobDecode(listMeta[index+int(headSeqLen):])
	return headSeq, tailSeq
}

func encodeListKey(seq, prevSeq, nextSeq *big.Float, key []byte) []byte {
	seqBuf, _ := seq.GobEncode()
	prevSeqBuf, _ := prevSeq.GobEncode()
	nextSeqBuf, _ := nextSeq.GobEncode()
	header := make([]byte, binary.MaxVarintLen64*3)
	var index int
	index += binary.PutVarint(header[index:], int64(len(seqBuf)))
	index += binary.PutVarint(header[index:], int64(len(prevSeqBuf)))
	index += binary.PutVarint(header[index:], int64(len(nextSeqBuf)))
	buf := make([]byte, index+len(seqBuf)+len(prevSeqBuf)+len(nextSeqBuf)+len(key))
	copy(buf[:index], header[:index])
	copy(buf[index:index+len(seqBuf)], seqBuf)
	copy(buf[index+len(seqBuf):index+len(seqBuf)+len(prevSeqBuf)], prevSeqBuf)
	copy(buf[index+len(seqBuf)+len(prevSeqBuf):index+len(seqBuf)+len(prevSeqBuf)+len(nextSeqBuf)], nextSeqBuf)
	copy(buf[index+len(seqBuf)+len(prevSeqBuf)+len(nextSeqBuf):], key)
	return buf
}

func decodeListKey(key []byte) ([]byte, *big.Float, *big.Float, *big.Float) {
	var index int
	seqLen, i := binary.Varint(key[index:])
	index += i
	prevSeqLen, i := binary.Varint(key[index:])
	index += i
	nextSeqLen, i := binary.Varint(key[index:])
	index += i
	seq, prevSeq, nextSeq := big.NewFloat(0), big.NewFloat(0), big.NewFloat(0)
	_ = seq.GobDecode(key[index : index+int(seqLen)])
	_ = prevSeq.GobDecode(key[index+int(seqLen) : index+int(seqLen)+int(prevSeqLen)])
	_ = nextSeq.GobDecode(key[index+int(seqLen)+int(prevSeqLen) : index+int(seqLen)+int(prevSeqLen)+int(nextSeqLen)])
	return key[index+int(seqLen)+int(prevSeqLen)+int(nextSeqLen):], seq,
		prevSeq, nextSeq
}
