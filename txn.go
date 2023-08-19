package CouloyDB

import (
	"container/heap"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
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
	if len(txn.strPendingWrites) == 0 && len(txn.hashPendingWrites) == 0 && len(txn.setPendingWrites) == 0 {
		return false
	}

	// go through all the old transactions looking for conflicts
	for _, committedTxn := range o.committedTxns {
		if committedTxn.commitTs <= txn.startTs {
			continue
		}

		// if the startTs is less than the commitTs of the committed transaction
		// possible transaction conflicts (especially dirty writing)
		for key := range txn.strPendingWrites {
			if _, has := committedTxn.strPendingWrites[key]; has {
				return true
			}
		}

		for key, pendingWrites := range txn.hashPendingWrites {
			for field := range pendingWrites {
				if _, has := committedTxn.hashPendingWrites[key][field]; has {
					return true
				}
			}
		}

		for key, pendingWrites := range txn.setPendingWrites {
			for member := range pendingWrites {
				if _, has := committedTxn.setPendingWrites[key][member]; has {
					return true
				}
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
	// Record operations on each data structure separately
	strPendingWrites  map[string]*pendingWrite
	hashPendingWrites map[string]map[string]*pendingWrite // key to field to pendingWrite
	setPendingWrites  map[string]map[string]*pendingWrite // key to member to pendingWrite

	listMetaPendingWrites map[string]*pendingWrite
	listDataPendingWrites map[string]map[string]*pendingWrite

	waitCommit *wait.Wait
}

func newTxn(readOnly bool, db *DB, isolationLevel IsolationLevel) *Txn {

	return &Txn{
		readOnly:              readOnly,
		db:                    db,
		isolationLevel:        isolationLevel,
		strPendingWrites:      make(map[string]*pendingWrite),
		hashPendingWrites:     make(map[string]map[string]*pendingWrite),
		setPendingWrites:      make(map[string]map[string]*pendingWrite),
		listMetaPendingWrites: make(map[string]*pendingWrite),
		listDataPendingWrites: make(map[string]map[string]*pendingWrite),
		waitCommit:            wait.NewWait(),
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

		// traverse the operations done by the transaction on each data structure
		go txn.updateStrIndex()
		go txn.updateHashIndex()
		go txn.updateListIndex()
		go txn.updateSetIndex()

		txn.waitCommit.Wait()

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

func (txn *Txn) updateStrIndex() {
	txn.waitCommit.Add(1)
	for key, pw := range txn.strPendingWrites {
		if pw.typ == data.LogRecordNormal {
			txn.db.index.getStrIndex().Put([]byte(key), pw.LogPos)
		}
		if pw.typ == data.LogRecordDeleted {
			txn.db.index.getStrIndex().Del([]byte(key))
		}
	}
	txn.waitCommit.Done()
}

func (txn *Txn) updateHashIndex() {
	txn.waitCommit.Add(1)
	for key, pendingWrites := range txn.hashPendingWrites {

		idx, ok := txn.db.index.getHashIndex(key)
		if !ok {
			txn.db.index.setHashIndex(key, meta.NewMemTable(txn.db.options.IndexType))
			idx, _ = txn.db.index.getHashIndex(key)
		}

		for field, pw := range pendingWrites {
			if pw.typ == data.LogRecordNormal {
				idx.Put([]byte(field), pw.LogPos)
			}
			if pw.typ == data.LogRecordDeleted {
				idx.Del([]byte(field))
			}
		}
	}
	txn.waitCommit.Done()
}

func (txn *Txn) updateSetIndex() {
	txn.waitCommit.Add(1)
	for key, pendingWrites := range txn.setPendingWrites {

		idx, ok := txn.db.index.getSetIndex(key)
		if !ok {
			txn.db.index.setSetIndex(key, meta.NewMemTable(txn.db.options.IndexType))
			idx, _ = txn.db.index.getSetIndex(key)
		}

		for hashKey, pw := range pendingWrites {
			if pw.typ == data.LogRecordNormal {
				idx.Put([]byte(hashKey), pw.LogPos)
			}
			if pw.typ == data.LogRecordDeleted {
				idx.Del([]byte(hashKey))
			}
		}
	}
	txn.waitCommit.Done()
}

func (txn *Txn) updateListIndex() {
	txn.waitCommit.Add(1)
	for key, pw := range txn.listMetaPendingWrites {
		if pw.typ == data.LogRecordNormal {
			txn.db.index.getListMetaIndex().Put([]byte(key), pw.LogPos)
		} else {
			txn.db.index.getListMetaIndex().Del([]byte(key))
		}
	}
	for key, pendingWrites := range txn.listDataPendingWrites {
		index, ok := txn.db.index.getListDataIndex(key)
		if !ok {
			txn.db.index.setListDataIndex(key, meta.NewMemTable(txn.db.options.IndexType))
			index, _ = txn.db.index.getListDataIndex(key)
		}

		for seq, pw := range pendingWrites {
			if pw.typ == data.LogRecordNormal {
				index.Put([]byte(seq), pw.LogPos)
			} else {
				index.Del([]byte(seq))
			}
		}
	}
	txn.waitCommit.Done()
}

// Get the key first in pendingWrites, if not then in db
func (txn *Txn) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, public.ErrKeyIsEmpty
	}
	if pos, ok := txn.strPendingWrites[string(key)]; ok {
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

	pos := txn.db.index.getStrIndex().Get(key)
	if pos == nil {
		return nil, public.ErrKeyNotFound
	}

	return txn.db.getValueByPos(pos)
}

// Set writes data to the db, but instead of writing it back to memtable, it writes to pendingWrites first
func (txn *Txn) Set(key []byte, value []byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	logRecord := &data.LogRecord{
		Key:      encodeKeyWithTxId(key, txn.startTs),
		Value:    value,
		Type:     data.LogRecordNormal,
		DataType: data.String,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	txn.strPendingWrites[string(key)] = &pendingWrite{typ: data.LogRecordNormal, LogPos: pos}
	return nil
}

// Del delete data to the db, but instead of writing it back to memtable, it writes to pendingWrites first
func (txn *Txn) Del(key []byte) error {
	if txn.readOnly {
		return public.ErrUpdateInReadOnlyTxn
	}
	logRecord := &data.LogRecord{
		Key:      encodeKeyWithTxId(key, txn.startTs),
		Type:     data.LogRecordDeleted,
		DataType: data.String,
	}
	pos, err := txn.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	txn.strPendingWrites[string(key)] = &pendingWrite{typ: data.LogRecordDeleted, LogPos: pos}
	return nil
}

func (txn *Txn) SetNX(key, value []byte) error {
	v, err := txn.Get(key)
	if v != nil {
		return public.ErrKeyExist
	}

	if err != nil && err != public.ErrKeyNotFound {
		return err
	}

	return txn.Set(key, value)
}

func (txn *Txn) SetEX(key, value []byte) error {
	_, err := txn.Get(key)
	if err != nil {
		return err
	}
	return txn.Set(key, value)
}

func (txn *Txn) GetSet(key, value []byte) ([]byte, error) {
	oldVal, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	err = txn.Set(key, value)
	if err != nil {
		return nil, err
	}
	return oldVal, nil
}

func (txn *Txn) StrLen(key []byte) (int, error) {
	value, err := txn.Get(key)
	if err != nil {
		return 0, err
	}
	return len(value), nil
}

func (txn *Txn) Incr(key []byte) (int, error) {
	return txn.incrOrDecr(key, true, 1)
}

func (txn *Txn) Decr(key []byte) (int, error) {
	return txn.incrOrDecr(key, false, 1)
}

func (txn *Txn) IncrBy(key []byte, delta int) (int, error) {
	return txn.incrOrDecr(key, true, delta)
}

func (txn *Txn) DecrBy(key []byte, delta int) (int, error) {
	return txn.incrOrDecr(key, false, delta)
}

func (txn *Txn) incrOrDecr(key []byte, isIncr bool, delta int) (int, error) {
	var (
		newVal []byte
		i      int
		err    error
	)

	oldVal, err := txn.Get(key)
	if err != nil {
		if err == public.ErrKeyNotFound {
			if isIncr {
				i = delta
				newVal = []byte(strconv.Itoa(delta))
			} else {
				i = -delta
				newVal = []byte(strconv.Itoa(-delta))
			}
		} else {
			return 0, err
		}
	} else {
		i, err = strconv.Atoi(string(oldVal))
		if err != nil {
			return 0, err
		}
		if isIncr {
			i += delta
		} else {
			i -= delta
		}
		newVal = []byte(strconv.Itoa(i))
	}

	err = txn.Set(key, newVal)
	if err != nil {
		return 0, err
	}
	return i, nil
}

func (txn *Txn) Exist(key []byte) bool {
	if pos, ok := txn.strPendingWrites[string(key)]; ok {
		if pos.typ != data.LogRecordDeleted {
			return true
		}
		return false
	}

	if pos := txn.db.index.getStrIndex().Get(key); pos != nil {
		return true
	}

	return false
}

func (txn *Txn) Append(key []byte, value []byte) error {
	if !txn.Exist(key) {
		return txn.Set(key, value)
	}
	v, err := txn.Get(key)
	if err != nil {
		return err
	}
	v = append(v, value...)
	return txn.Set(key, v)
}

func (txn *Txn) MGet(keys [][]byte) ([][]byte, error) {
	values := make([][]byte, len(keys))

	for i, key := range keys {
		value, err := txn.Get(key)
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

func (txn *Txn) MSet(args [][]byte) error {
	if len(args)%2 != 0 {
		return public.ErrTxnArgsWrong
	}

	for i := 0; i < len(args); i += 2 {
		err := txn.Set(args[i], args[i+1])
		if err != nil {
			return err
		}
	}

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
