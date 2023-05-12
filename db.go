package CouloyDB

import (
	"encoding/binary"
	"errors"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/gofrs/flock"
	lua "github.com/yuin/gopher-lua"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DB struct {
	options      Options
	activityFile *data.DataFile
	oldFile      map[uint32]*data.DataFile
	memTable     meta.MemTable
	mu           *sync.RWMutex
	txId         int64
	isMerging    bool
	flock        *flock.Flock
	bytesWrite   uint64
	mergeChan    chan struct{}
	L            *lua.LState
	oracle       *oracle
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
		options:  opt,
		oldFile:  make(map[uint32]*data.DataFile),
		memTable: meta.NewMemTable(opt.IndexType),
		mu:       new(sync.RWMutex),
		flock:    fl,
	}

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

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return public.ErrKeyIsEmpty
	}
	if len(key) == 1 && (key[0] < 32 || key[0] == 127) {
		return public.ErrKeyIsControlChar
	}
	logRecord := &data.LogRecord{
		Key:   encodeKeyWithTxId(key, public.NO_TX_ID),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if ok := db.memTable.Put(key, pos); !ok {
		return public.ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, public.ErrKeyIsEmpty
	}
	pos := db.memTable.Get(key)
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
	if pos := db.memTable.Get(key); pos == nil {
		return nil
	}

	// Build deleted tags LogRecord
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(key, public.NO_TX_ID),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// Delete key in memory memTable
	if ok := db.memTable.Del(key); !ok {
		return public.ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) IsExist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, public.ErrKeyIsEmpty
	}
	// Check if exist in memory memTable
	if pos := db.memTable.Get(key); pos == nil {
		return false, public.ErrKeyNotFound
	}
	return true, nil
}

func (db *DB) Size() int {
	return db.memTable.Count()
}

// ListKeys get all the key and return
func (db *DB) ListKeys() [][]byte {
	iterator := db.memTable.Iterator(false)
	keys := make([][]byte, db.memTable.Count())
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
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.memTable.Iterator(false)
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

	db.memTable = meta.NewMemTable(db.options.IndexType)
	return nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.flock.Unlock(); err != nil {
			panic(err)
		}
	}()
	if db.activityFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.activityFile.Close(); err != nil {
		return err
	}

	for _, file := range db.oldFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
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
			_ = db.Merge()

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

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogPos) {
		if typ == data.LogRecordDeleted {
			db.memTable.Del(key)
		} else {
			db.memTable.Put(key, pos)
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
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {

				if logRecord.Type == data.LogRecordTxnBegin {
					// if "begin" do nothing
				} else if logRecord.Type == data.LogRecordTxnCommit {
					// if the tx has finished, update to memIndex
					for _, txRecord := range txRecords[txId] {
						updateIndex(txRecord.Record.Key, txRecord.Record.Type, txRecord.Pos)
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

func (db *DB) getValueByPos(pos *data.LogPos) ([]byte, error) {
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

func deleteLessThan[T comparable](s []T, val T) []T {
	for i, v := range s {
		if v == val {
			return s[i:]
		}
	}
	return s
}
