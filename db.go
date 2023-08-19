package CouloyDB

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/ds"
	"github.com/gofrs/flock"
	lua "github.com/yuin/gopher-lua"
)

type DB struct {
	options      Options
	activityFile *data.DataFile
	oldFile      map[uint32]*data.DataFile
	index        *index
	indexLocks   map[data.DataType]*sync.RWMutex
	mu           *sync.RWMutex
	txId         int64
	isMerging    bool
	flock        *flock.Flock
	bytesWrite   uint64
	mergeChan    chan struct{}
	mergeDone    chan error
	L            *lua.LState
	oracle       *oracle
	ttl          *ttl
	wm           *watcherManager
}

func NewCouloyDB(opt Options) (*DB, error) {
	// Verify configuration items
	err := checkOptions(&opt)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(opt.DirPath); err != nil {
		if err := os.MkdirAll(opt.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	fl := flock.New(filepath.Join(opt.DirPath, public.FileLockName))
	if getLock, err := fl.TryLock(); err != nil {
		return nil, err
	} else if !getLock {
		return nil, public.ErrDirOccupied
	}

	// Init DB
	db := &DB{
		options: opt,
		oldFile: make(map[uint32]*data.DataFile),
		index: &index{
			hashIndex: make(map[string]meta.MemTable),
			setIndex:  make(map[string]meta.MemTable),
			strIndex:  meta.NewMemTable(opt.IndexType),
			listIndex: listIndex{
				metaIndex: meta.NewMemTable(opt.IndexType),
				dataIndex: make(map[string]meta.MemTable),
			},
		},
		indexLocks: make(map[data.DataType]*sync.RWMutex),
		mu:         new(sync.RWMutex),
		mergeChan:  make(chan struct{}),
		mergeDone:  make(chan error),
		flock:      fl,
		wm:         newWatcherManager(),
	}

	db.indexLocks[data.String] = &sync.RWMutex{}
	db.indexLocks[data.Hash] = &sync.RWMutex{}
	db.indexLocks[data.Set] = &sync.RWMutex{}

	db.ttl = newTTL(func(key string) error {
		return db.Del([]byte(key))
	})

	if opt.EnableLuaInterpreter {
		db.initLuaInterpreter()
	}

	db.initOracle()
	// load merge file dir
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// Load DataFile and memTable
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	go db.mergeWorker()

	go db.ttl.start()

	go db.wm.start()

	return db, nil
}

func (db *DB) PutWithExpiration(key, value []byte, duration time.Duration) error {
	return db.put(key, value, duration)
}

func (db *DB) Put(key, value []byte) error {
	return db.put(key, value, 0)
}

func (db *DB) put(key, value []byte, duration time.Duration) error {
	if err := checkKey(key); err != nil {
		return err
	}

	db.getIndexLockByType(data.String).Lock()
	defer db.getIndexLockByType(data.String).Unlock()

	var expiration int64
	if duration != 0 {
		expiration = time.Now().Add(duration).UnixNano()
		db.ttl.add(ds.NewJob(string(key), time.Unix(0, expiration)))
	} else {
		// If it is a key without an expiration time set
		// you may need to remove the previously set expiration time
		db.ttl.del(string(key))
	}

	logRecord := &data.LogRecord{
		Key:        encodeKeyWithTxId(key, public.NO_TX_ID),
		Value:      value,
		Type:       data.LogRecordNormal,
		DataType:   data.String,
		Expiration: expiration,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	db.Notify(string(key), value, PutEvent)

	if ok := db.index.getStrIndex().Put(key, pos); !ok {
		return public.ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, public.ErrKeyIsEmpty
	}

	db.getIndexLockByType(data.String).RLock()
	defer db.getIndexLockByType(data.String).RUnlock()

	if db.ttl.isExpired(string(key)) {
		// if the key is expired, just return and don't delete the key now
		return nil, public.ErrKeyNotFound
	}

	pos := db.index.getStrIndex().Get(key)
	if pos == nil {
		return nil, public.ErrKeyNotFound
	}

	return db.getValueByPos(pos)
}

func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return public.ErrKeyIsEmpty
	}
	// Check if exist in memory memTable

	db.getIndexLockByType(data.String).Lock()
	defer db.getIndexLockByType(data.String).Unlock()

	if pos := db.index.getStrIndex().Get(key); pos == nil {
		return nil
	}

	db.ttl.del(string(key))

	// Build deleted tags LogRecord
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(key, public.NO_TX_ID),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	db.Notify(string(key), nil, DelEvent)

	// Delete key in memory memTable
	if ok := db.index.getStrIndex().Del(key); !ok {
		return public.ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) IsExist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, public.ErrKeyIsEmpty
	}
	db.getIndexLockByType(data.String).RLock()
	defer db.getIndexLockByType(data.String).RUnlock()
	// Check if exist in memory memTable
	if pos := db.index.getStrIndex().Get(key); pos == nil {
		return false, public.ErrKeyNotFound
	}
	return true, nil
}

func (db *DB) Size() int {
	db.getIndexLockByType(data.String).RLock()
	defer db.getIndexLockByType(data.String).RUnlock()
	// may calculate expired key
	return db.index.getStrIndex().Count()
}

// ListKeys get all the key and return
func (db *DB) ListKeys() [][]byte {
	db.getIndexLockByType(data.String).RLock()
	defer db.getIndexLockByType(data.String).RUnlock()
	iterator := db.index.getStrIndex().Iterator(false)
	keys := make([][]byte, db.index.getStrIndex().Count())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold gets all the keys and executes the function passed in by the user.
// Terminates the traversal when the function returns false
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.getIndexLockByType(data.String).RLock()
	defer db.getIndexLockByType(data.String).RUnlock()
	iterator := db.index.getStrIndex().Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPos(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Clear() error {
	err := db.Close()
	if err != nil {
		return err
	}

	err = os.RemoveAll(db.options.DirPath)
	if err != nil {
		return err
	}

	fl := flock.New(filepath.Join(db.options.DirPath, public.FileLockName))
	if getLock, err := fl.TryLock(); err != nil {
		return err
	} else if !getLock {
		return public.ErrDirOccupied
	}

	db.index.strIndex = meta.NewMemTable(db.options.IndexType)
	db.index.hashIndex = make(map[string]meta.MemTable)
	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.flock.Unlock(); err != nil {
			panic(err)
		}
	}()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.activityFile == nil {
		return nil
	}

	if err := db.activityFile.Close(); err != nil {
		return err
	}

	for _, file := range db.oldFile {
		if err := file.Close(); err != nil {
			return err
		}
	}

	db.ttl.stop()

	db.wm.stop()
	return nil
}

func (db *DB) Sync() error {
	if db.activityFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activityFile.Sync()
}

func (db *DB) mergeWorker() {
	var mergeInterval time.Duration
	var mergeTicker *time.Ticker
	var needTicker bool

	// The minimum interval for merging is 1 minute. Otherwise, the merge is not scheduled by default
	if db.options.MergeInterval >= 60 {
		mergeInterval = time.Duration(db.options.MergeInterval) * time.Second
		mergeTicker = time.NewTicker(mergeInterval)
		needTicker = true
	} else {
		mergeTicker = time.NewTicker(1)
		mergeTicker.Stop()
		needTicker = false
	}
	for {
		select {
		case <-db.mergeChan:
			db.mergeDone <- db.merge()
			if needTicker {
				mergeTicker.Reset(mergeInterval)
			}
		case <-mergeTicker.C:
			_ = db.Merge()
		}
	}
}

func (db *DB) appendLogRecordWithLock(log *data.LogRecord) (*data.LogPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(log)
}

func (db *DB) appendLogRecord(log *data.LogRecord) (*data.LogPos, error) {

	if db.activityFile == nil {
		if err := db.setActivityFile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := data.EncodeLogRecord(log)
	if db.activityFile.WriteOff+size > db.options.DataFileSize {
		if err := db.activityFile.Sync(); err != nil {
			return nil, err
		}
		db.oldFile[db.activityFile.FileId] = db.activityFile

		if err := db.setActivityFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activityFile.WriteOff
	if err := db.activityFile.Write(encRecord); err != nil {
		return nil, err
	}

	var needSync bool
	if db.options.SyncWrites {
		needSync = true
	} else if db.options.BytesPerSync > 0 {
		atomic.AddUint64(&db.bytesWrite, uint64(size))
		if db.bytesWrite > db.options.BytesPerSync {
			atomic.StoreUint64(&db.bytesWrite, 0)
			needSync = true
		}
	}

	if needSync {
		if err := db.activityFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogPos{
		Fid:    db.activityFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

func (db *DB) setActivityFile() error {
	var initialFileId uint32 = 0
	if db.activityFile != nil {
		initialFileId = db.activityFile.FileId + 1
	}
	// open the new dataFile
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activityFile = dataFile
	return nil
}

func checkOptions(opt *Options) error {
	if opt.DirPath == "" {
		return errors.New("DirPath can not be empty")
	}
	if opt.DataFileSize < 0 {
		return errors.New("DataFileSize must be greater than 0")
	}
	if opt.DataFileSize < 64 {
		opt.DataFileSize = 64
	}
	return nil
}

func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	// Iterate through all the files in the directory and find the ones that end with .cly
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), public.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return errors.New("the data dir maybe contaminated or damaged")
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)

	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activityFile = dataFile
		} else {
			db.oldFile[uint32(fid)] = dataFile
		}
	}

	// load index from hint file first
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}

	// loadIndex
	if err := db.loadIndex(fileIds); err != nil {
		return err
	}
	return nil
}

func (db *DB) loadIndex(fids []int) error {
	if len(fids) == 0 {
		return nil
	}

	// get the non merge file
	hasMerge, nonMergeFileId := false, 0
	mergeFinFileName := filepath.Join(db.options.DirPath, public.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = int(fid)
	}

	// only read non merge file
	if hasMerge {
		deleteLessThan(fids, nonMergeFileId)
	}

	expirations := make(map[string]int64)

	updateIndex := func(key []byte, log *data.LogRecord, pos *data.LogPos) {
		switch log.DataType {
		case data.String:
			if log.Type == data.LogRecordDeleted {
				delete(expirations, string(key))
				db.index.getStrIndex().Del(key)
			} else {
				expirations[string(key)] = log.Expiration
				db.index.getStrIndex().Put(key, pos)
			}
		case data.Hash:
			realKey, field := decodeFieldKey(log.Key)
			var (
				idx meta.MemTable
				ok  bool
			)
			if idx, ok = db.index.getHashIndex(string(realKey)); !ok {
				db.index.setHashIndex(string(realKey), meta.NewMemTable(db.options.IndexType))
				idx, _ = db.index.getHashIndex(string(realKey))
			}
			if log.Type == data.LogRecordDeleted {
				idx.Del(field)
			} else {
				idx.Put(field, pos)
			}
		case data.List:
			realKey, seq, _, _ := decodeListKey(log.Key)
			seqBuf, _ := seq.GobEncode()
			var (
				idx meta.MemTable
				ok  bool
			)
			if idx, ok = db.index.getListDataIndex(string(realKey)); !ok {
				db.index.setListDataIndex(string(realKey), meta.NewMemTable(db.options.IndexType))
				idx, _ = db.index.getListDataIndex(string(realKey))
			}
			if log.Type == data.LogRecordDeleted {
				idx.Del(seqBuf)
			} else {
				idx.Put(seqBuf, pos)
			}
		case data.ListMeta:
			if log.Type == data.LogRecordDeleted {
				db.index.getListMetaIndex().Del(key)
			} else {
				db.index.getListMetaIndex().Put(key, pos)
			}
		case data.Set:
			realKey, member := decodeMemberKey(log.Key)
			var (
				idx meta.MemTable
				ok  bool
			)
			if idx, ok = db.index.getSetIndex(string(realKey)); !ok {
				db.index.setSetIndex(string(realKey), meta.NewMemTable(db.options.IndexType))
				idx, _ = db.index.getSetIndex(string(realKey))
			}
			hashKey := hashMemberKey(realKey, member)
			if log.Type == data.LogRecordDeleted {
				idx.Del(hashKey)
			} else {
				idx.Put(hashKey, pos)
			}
		}
	}

	// a map to store the Record data in tx temporarily
	// txId -> recordList
	txRecords := make(map[int64][]*data.TxRecord)

	// Iterate through all the file ids and process the records in the file
	for i, fid := range fids {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activityFile.FileId {
			dataFile = db.activityFile
		} else {
			dataFile = db.oldFile[fileId]
		}
		var offset int64
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// Building in-memory indexes
			logRecordPos := &data.LogPos{Fid: fileId, Offset: offset}

			realKey, txId := parseLogRecordKey(logRecord.Key)
			if txId == public.NO_TX_ID {
				// if not in tx, update memIndex directly
				updateIndex(realKey, logRecord, logRecordPos)
			} else {

				if logRecord.Type == data.LogRecordTxnBegin {
					// if "begin" do nothing
				} else if logRecord.Type == data.LogRecordTxnCommit {
					// if the tx has finished, update to memIndex
					for _, txRecord := range txRecords[txId] {
						updateIndex(txRecord.Record.Key, txRecord.Record, txRecord.Pos)
					}
					delete(txRecords, txId)
				} else if logRecord.Type == data.LogRecordTxnRollback {
					delete(txRecords, txId)
				} else {
					//
					logRecord.Key = realKey
					txRecords[txId] = append(txRecords[txId], &data.TxRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// Increment the offset, starting from the new position
			offset += size
		}

		// If this is the active file, update its WriteOffset
		if i == len(fids)-1 {
			db.activityFile.WriteOff = offset
		}
	}

	// update ttl according to the current memtable
	for key, expiration := range expirations {
		if expiration != 0 {
			exp := time.Unix(0, expiration)
			if exp.After(time.Now()) {
				db.ttl.add(ds.NewJob(key, exp))
			} else {
				err := db.Del([]byte(key))
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// func (db DB) loadTxFile() error {
//	fileName := filepath.Join(db.options.DirPath, public.TxIDFileName)
//	if _, err := os.Stat(fileName); os.IsNotExist(err) {
//		return nil
//	}
//	txIDFile, err := data.OpenTxIDFile(db.options.DirPath)
//	if err != nil {
//		return err
//	}
//	record, _, err := txIDFile.ReadLogRecord(0)
//	txID, err := strconv.ParseUint(string(record.Value), 10, 64)
//	if err != nil {
//		return err
//	}
//	db.txId = txID
//
//	return os.Remove(fileName)
// }

func (db *DB) getLogRecordByPos(pos *data.LogPos) (*data.LogRecord, error) {
	var dataFile *data.DataFile
	if db.activityFile.FileId == pos.Fid {
		dataFile = db.activityFile
	} else {
		dataFile = db.oldFile[pos.Fid]
	}
	if dataFile == nil {
		return nil, public.ErrKeyNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, public.ErrKeyNotFound
	}
	return logRecord, nil
}

func (db *DB) getValueByPos(pos *data.LogPos) ([]byte, error) {
	logRecord, err := db.getLogRecordByPos(pos)
	if err != nil {
		return nil, err
	}
	return logRecord.Value, nil
}

// prase LogRecord's Key and get the real key with txId
func parseLogRecordKey(key []byte) ([]byte, int64) {
	txId, n := binary.Varint(key)
	realKey := key[n:]
	return realKey, txId
}

func (db *DB) GetTxId() int64 {
	return db.oracle.GetTxId()
}

func (db *DB) Persist(key []byte) {
	db.ttl.del(string(key))
}

func (db *DB) Watch(ctx context.Context, key string) <-chan *watchEvent {
	return db.wm.watch(ctx, key)
}

func (db *DB) UnWatch(watcher *Watcher) {
	db.wm.unWatch(watcher)
}

func (db *DB) Notify(key string, value []byte, entryType eventType) {
	if db.wm.watched(key) {
		db.wm.notify(&watchEvent{key: key, value: value, eventType: entryType})
	}
}

func (db *DB) getIndexLockByType(typ data.DataType) *sync.RWMutex {
	switch typ {
	case data.String:
		return db.indexLocks[data.String]
	case data.Hash:
		return db.indexLocks[data.Hash]
	case data.Set:
		return db.indexLocks[data.Set]
	}
	return nil
}

func checkKey(key []byte) error {
	if len(key) == 0 {
		return public.ErrKeyIsEmpty
	}
	if len(key) == 1 && (key[0] < 32 || key[0] == 127) {
		return public.ErrKeyIsControlChar
	}
	return nil
}

func deleteLessThan[T comparable](s []T, val T) []T {
	for i, v := range s {
		if v == val {
			return s[i:]
		}
	}
	return s
}
