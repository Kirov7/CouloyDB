package CouloyDB

import (
	"container/heap"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"sync"
	"sync/atomic"
	"time"
)

type IsolationLevel uint8

const (
	ReadCommitted IsolationLevel = iota
	Serializable
)

// Global transaction manager
type oracle struct {
	mu *sync.RWMutex
	// Unique transaction ID
	txId int64

	// A minimum heap for maintaining active transactions
	activeTxnHeap int64Heap
	// The committed transaction list used for conflict detection
	committedTxns []*Txn
}

func (db *DB) initOracle() *oracle {
	o := &oracle{
		mu: &sync.RWMutex{},
		// When CouloyDB is started, the current timestamp is taken as the initial transaction id,
		// and the atoms increment on this basis each time a new transaction id is fetched
		txId:          time.Now().UnixNano(),
		activeTxnHeap: int64Heap{},
		committedTxns: make([]*Txn, 0),
	}
	db.oracle = o
	return o
}

func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.pendingWrites) == 0 {
		return false
	}

	// go through all the old transactions looking for conflicts
	for _, committedTxn := range o.committedTxns {
		if committedTxn.commitTs <= txn.startTs {
			continue
		}
		// if the startTs is less than the commitTs of the committed transaction
		// possible transaction conflicts (especially dirty writing)
		for k := range txn.pendingWrites {
			if _, has := committedTxn.pendingWrites[k]; has {
				return true
			}
		}
	}

	return false
}

func (o *oracle) newCommit(txn *Txn) {
	// Clean up overdue submission records
	o.cleanupCommitTxn()
	// Get the commit timestamp
	txn.commitTs = o.GetTxId()
	o.committedTxns = append(o.committedTxns, txn)
	// Remove this transaction from the active transaction minimum heap
	o.removeActiveTxn(txn.startTs)

	if txn.isolationLevel == Serializable {
		txn.unlock()
	}
}

func (o *oracle) newBegin(txn *Txn) {
	// Get the start timestamp
	txn.startTs = txn.db.oracle.GetTxId()
	// Insert this transaction from the active transaction minimum heap
	o.addActiveTxn(txn.startTs)
}

func (o *oracle) GetTxId() int64 {
	return atomic.AddInt64(&o.txId, 1)
}

// Clears all transactions whose commitTs is less than the minimum startTs in all active transactions
func (o *oracle) cleanupCommitTxn() {
	// Get the minimum transaction timestamp from the active transaction timestamp
	startTs, err := o.peekActiveTxn()
	if err != nil {
		return
	}

	tmp := o.committedTxns[:0]
	for _, txn := range o.committedTxns {
		if txn.commitTs <= startTs {
			continue
		}
		tmp = append(tmp, txn)
	}
	o.committedTxns = tmp
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
	// Backreference DB instance
	db *DB
	// Isolation level of txn
	isolationLevel IsolationLevel
	// Transaction start time stamp, obtained at begin
	startTs int64
	// Transaction commit time stamp, obtained at commit
	commitTs int64

	// The data written is stored temporarily in pendingWrites instead of memtable
	pendingWrites map[string]pendingWrite
}

func newTxn(readOnly bool, db *DB, isolationLevel IsolationLevel) *Txn {
	return &Txn{
		readOnly:       readOnly,
		db:             db,
		isolationLevel: isolationLevel,
		pendingWrites:  make(map[string]pendingWrite),
	}
}

type pendingWrite struct {
	typ data.LogRecordType
	*data.LogPos
}

// SerialTransaction serializable transaction
// For now, the commit of a serializable transaction is unlikely to conflict
// so no retry is required
func (db *DB) SerialTransaction(readOnly bool, fn func(txn *Txn) error) error {
	if fn == nil {
		return public.ErrTxnFnEmpty
	}
	txn := newTxn(readOnly, db, Serializable)
	txn.begin()
	if err := fn(txn); err != nil {
		txn.rollback()
		return err
	}
	if err := txn.commit(); err != nil {
		return err
	}
	return nil
}

// RWTransaction Read/Write transaction
// if retryOnConflict is true, then the transaction will automatically retry until the transaction commits correctly
// fn is the real transaction that you want to perform
func (db *DB) RWTransaction(retryOnConflict bool, fn func(txn *Txn) error) error {
	for {
		tx := newTxn(false, db, ReadCommitted)

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

		if err == public.ErrTransactionConflict && retryOnConflict {
			continue
		}
		return err
	}
}

// begin
func (txn *Txn) begin() {
	// the real begin
	txn.db.oracle.newBegin(txn)

	if txn.isolationLevel == Serializable {
		txn.lock()
	}

	// write the begin-mark to datafile
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(public.TX_BEGIN_KEY, txn.startTs),
		Type: data.LogRecordTxnBegin,
	}
	_, _ = txn.db.appendLogRecordWithLock(logRecord)
}

// Check for conflicts and finally perform a commit or rollback
func (txn *Txn) commit() error {
	// because activeTxnHeap and committedTxns are not concurrent secure locks
	// so the locks should be obtained first when commit
	if txn.isolationLevel == ReadCommitted {
		txn.db.oracle.mu.Lock()
		defer txn.db.oracle.mu.Unlock()
	}
	// check whether data conflicts exist
	if txn.isolationLevel == Serializable || !txn.db.oracle.hasConflict(txn) {
		// write the commit-mark to datafile
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
		// the real commit
		txn.db.oracle.newCommit(txn)
		//fmt.Printf("=== %d === commit\n", id(txn.startTs))
		return nil
	}

	// if there has a conflict, roll back
	txn.rollback()
	//fmt.Printf("=== %d === rollback(has conflict)\n", id(txn.startTs))
	return public.ErrTransactionConflict
}

// rollback
func (txn *Txn) rollback() {
	// write the rollback-mark to datafile
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(public.TX_ROLLBACK_KEY, txn.startTs),
		Type: data.LogRecordTxnRollback,
	}
	_, _ = txn.db.appendLogRecordWithLock(logRecord)
	if txn.isolationLevel == Serializable {
		txn.unlock()
	}
}

// Get the key first in pendingWrites, if not then in db
func (txn *Txn) Get(key []byte) ([]byte, error) {
	if pos, ok := txn.pendingWrites[string(key)]; ok {
		// If the key is found, check to see if it has been deleted
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

// Put writes data to the db, but instead of writing it back to memtable, it writes to pendingWrites first
func (txn *Txn) Put(key []byte, value []byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
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

// Del delete data to the db, but instead of writing it back to memtable, it writes to pendingWrites first
func (txn *Txn) Del(key []byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
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

func (txn *Txn) lock() {
	if txn.readOnly {
		txn.db.oracle.mu.RLock()
	} else {
		txn.db.oracle.mu.Lock()
	}
}

func (txn *Txn) unlock() {
	if txn.readOnly {
		txn.db.oracle.mu.RUnlock()
	} else {
		txn.db.oracle.mu.Unlock()
	}
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
