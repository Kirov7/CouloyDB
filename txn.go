package CouloyDB

import (
	"container/heap"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"sync"
	"sync/atomic"
	"time"
)

type oracle struct {
	mu   *sync.RWMutex
	txId int64

	activeTxnHeap int64Heap
	commitedTxns  []*Txn
}

func (db *DB) initOracle() *oracle {
	o := &oracle{
		mu:            &sync.RWMutex{},
		txId:          time.Now().UnixNano(),
		activeTxnHeap: int64Heap{},
		commitedTxns:  make([]*Txn, 0),
	}
	db.oracle = o
	return o
}

func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.pendingWrites) == 0 {
		return false
	}

	// go through all the old transactions looking for conflicts
	for _, committedTxn := range o.commitedTxns {
		if committedTxn.commitTs <= txn.startTs {
			continue
		}
		// if the startTs is less than the commitTs of the committed transaction
		// possible transaction conflicts (especially dirty writing)
		for k, _ := range txn.pendingWrites {
			if _, has := committedTxn.pendingWrites[k]; has {
				return true
			}
		}
	}

	return false
}

func (o *oracle) newCommit(txn *Txn) {
	// o.mu.Lock()
	// defer o.mu.Unlock()
	o.cleanupCommitTxn()
	txn.commitTs = o.GetTxId()
	o.commitedTxns = append(o.commitedTxns, txn)
	o.removeActiveTxn(txn.startTs)
}

func (o *oracle) newBegin(txn *Txn) {
	txn.startTs = txn.db.oracle.GetTxId()
	o.addActiveTxn(txn.startTs)
}

func (o *oracle) GetTxId() int64 {
	return atomic.AddInt64(&o.txId, 1)
}

// Clears all transactions whose commitTs is less than the minimum startTs in all active transactions
func (o *oracle) cleanupCommitTxn() {
	startTs, err := o.peekActiveTxn()
	if err != nil {
		return
	}

	tmp := o.commitedTxns[:0]
	for _, txn := range o.commitedTxns {
		if txn.commitTs <= startTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	o.commitedTxns = tmp
}

func (o *oracle) addActiveTxn(startTs int64) {

	heap.Push(&o.activeTxnHeap, startTs)
}

// peek Find the active transaction with the smallest start timestamp
func (o *oracle) peekActiveTxn() (int64, error) {

	if o.activeTxnHeap.Len() == 0 {
		return 0, public.ErrHeapEmpty
	}
	return o.activeTxnHeap[0], nil
}

// remove Removes the specified transaction from the active transaction heap
func (o *oracle) removeActiveTxn(x int64) bool {
	idx := -1
	for i, v := range o.activeTxnHeap {
		if v == x {
			idx = i
			break
		}
	}

	if idx == -1 {
		return false
	}

	heap.Remove(&o.activeTxnHeap, idx)
	return true
}

type Txn struct {
	readOnly bool
	db       *DB
	startTs  int64
	commitTs int64

	pendingWrites map[string]pendingWrite
}

type pendingWrite struct {
	typ data.LogRecordType
	*data.LogPos
}

func (db *DB) RWTransaction(retryOnErr bool, fn func(txn *Txn) error) error {
	for {
		tx := &Txn{
			readOnly:      false,
			db:            db,
			pendingWrites: make(map[string]pendingWrite),
		}

		tx.begin()
		err := fn(tx)
		if err != nil {
			tx.rollback()
			return err
		}
		err = tx.commit()
		if err == nil {
			return nil
		}

		if err == public.ErrTransactionConflict && retryOnErr {
			continue
		}
		return err
	}
}

func (txn *Txn) begin() {
	txn.startTs = txn.db.oracle.GetTxId()

	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(public.TX_BEGIN_KEY, txn.startTs),
		Type: data.LogRecordTxnBegin,
	}
	_, _ = txn.db.appendLogRecordWithLock(logRecord)
}

func (txn *Txn) commit() error {
	// check whether data conflicts exist
	txn.db.oracle.mu.Lock()
	defer txn.db.oracle.mu.Unlock()
	if !txn.db.oracle.hasConflict(txn) {
		logRecord := &data.LogRecord{
			Key:  encodeKeyWithTxId(public.TX_COMMIT_KEY, txn.startTs),
			Type: data.LogRecordTxnCommit,
		}
		_, err := txn.db.appendLogRecordWithLock(logRecord)
		if err != nil {
			return err
		}

		for key, pw := range txn.pendingWrites {
			if pw.typ == data.LogRecordNormal {
				txn.db.memTable.Put([]byte(key), pw.LogPos)
			}
			if pw.typ == data.LogRecordDeleted {
				txn.db.memTable.Del([]byte(key))
			}
		}
		txn.db.oracle.newCommit(txn)
		return nil
	}

	// if there has a conflict, roll back
	txn.rollback()
	return public.ErrTransactionConflict
}

func (txn *Txn) rollback() {
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(public.TX_ROLLBACK_KEY, txn.startTs),
		Type: data.LogRecordTxnRollback,
	}
	_, _ = txn.db.appendLogRecordWithLock(logRecord)
}

func (txn *Txn) Get(key []byte) ([]byte, error) {
	if pos, ok := txn.pendingWrites[string(key)]; ok {
		if pos.typ != data.LogRecordDeleted {
			v, err := txn.db.getValueByPos(pos.LogPos)
			if err != nil {
				return nil, err
			}
			return v, nil
		}
		return nil, public.ErrKeyNotFound
	}
	return txn.db.Get(key)
}

func (txn *Txn) Put(key []byte, value []byte) error {
	logRecord := &data.LogRecord{
		Key:   encodeKeyWithTxId(key, txn.startTs),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	txn.pendingWrites[string(key)] = pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
	return nil
}

func (txn *Txn) Del(key []byte) error {
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(key, txn.startTs),
		Type: data.LogRecordDeleted,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	txn.pendingWrites[string(key)] = pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	return nil
}

type int64Heap []int64

func (h int64Heap) Len() int            { return len(h) }
func (h int64Heap) Less(i, j int) bool  { return h[i] < h[j] }
func (h int64Heap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *int64Heap) Push(x interface{}) { *h = append(*h, x.(int64)) }
func (h *int64Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
