package CouloyDB

import (
	"encoding/binary"
	"errors"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type DB struct {
	options      Options
	activityFile *data.DataFile
	oldFile      map[uint32]*data.DataFile
	memTable     meta.MemTable
	mu           *sync.RWMutex
	txId         uint64
	isMerging    bool
	flock        *flock.Flock
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

	fl := flock.New(filepath.Join(opt.DirPath, fileLockName))
	if getLock, err := fl.TryLock(); err != nil {
		return nil, err
	} else if !getLock {
		return nil, ErrDirOccupied
	}

	// Init DB
	db := &DB{
		options:  opt,
		oldFile:  make(map[uint32]*data.DataFile),
		memTable: meta.NewMemTable(opt.IndexType),
		mu:       new(sync.RWMutex),
		flock:    fl,
	}

	// load merge file dir
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// Load DataFile and memTable
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// load txId from txFile
	if err := db.loadTxFile(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if len(key) == 1 && (key[0] < 32 || key[0] == 127) {
		return ErrKeyIsControlChar
	}
	logRecord := &data.LogRecord{
		Key:   encodeKeyWithTxId(key, NO_TX_ID),
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	if ok := db.memTable.Put(key, pos); !ok {
		return ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	pos := db.memTable.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	return db.getValueByPos(pos)
}

func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// Check if exist in memory memTable
	if pos := db.memTable.Get(key); pos == nil {
		return nil
	}

	// Build deleted tags LogRecord
	logRecord := &data.LogRecord{
		Key:  encodeKeyWithTxId(key, NO_TX_ID),
		Type: data.LogRecordDeleted,
	}

	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// Delete key in memory memTable
	if ok := db.memTable.Del(key); !ok {
		return ErrUpdateIndexFailed
	}
	return nil
}

// ListKeys get all the key and return
func (db *DB) ListKeys() [][]byte {
	iterator := db.memTable.Iterator(false)
	keys := make([][]byte, db.memTable.Count())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
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

func (db *DB) Close() error {
	if db.activityFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	defer func() {
		if err := db.flock.Unlock(); err != nil {
			panic(err)
		}
	}()

	// store current txID
	txFile, err := data.OpenTxIDFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   TX_PERSIST_KEY,
		Value: []byte(strconv.FormatUint(db.txId, 10)),
	}
	encodeLogRecord, _ := data.EncodeLogRecord(record)
	if err := txFile.Write(encodeLogRecord); err != nil {
		return err
	}
	if err := txFile.Sync(); err != nil {
		return err
	}

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
	if db.options.SyncWrites {
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
	if db.activityFile == nil {
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
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
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
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
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
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.memTable.Del(key)
		} else {
			ok = db.memTable.Put(key, pos)
		}
		if !ok {
			panic("update memTable failed at update")
			//return ErrUpdateIndexFailed
		}
	}

	// a map to store the Record data in tx temporarily
	// txId -> recordList
	txRecords := make(map[uint64][]*data.TxRecord)
	var curTxId = NO_TX_ID

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
			if txId == NO_TX_ID {
				// if not in tx, update memIndex directly
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// if the tx has finished, update to memIndex
				if logRecord.Type == data.LogRecordTxnFin {
					for _, txRecord := range txRecords[txId] {
						updateIndex(txRecord.Record.Key, txRecord.Record.Type, txRecord.Pos)

					}
					delete(txRecords, txId)
				} else {
					//
					logRecord.Key = realKey
					txRecords[txId] = append(txRecords[txId], &data.TxRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
				// update txId
				if txId > curTxId {
					curTxId = txId
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
	db.txId = curTxId
	return nil
}

func (db DB) loadTxFile() error {
	fileName := filepath.Join(db.options.DirPath, data.TxIDFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	txIDFile, err := data.OpenTxIDFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := txIDFile.ReadLogRecord(0)
	txID, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.txId = txID

	return os.Remove(fileName)
}
func (db *DB) getValueByPos(pos *data.LogPos) ([]byte, error) {
	var dataFile *data.DataFile
	if db.activityFile.FileId == pos.Fid {
		dataFile = db.activityFile
	} else {
		dataFile = db.oldFile[pos.Fid]
	}
	if dataFile == nil {
		return nil, ErrKeyNotFound
	}

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// prase LogRecord's Key and get the real key with txId
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	txId, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, txId
}

func (db *DB) GetTxId() uint64 {
	return atomic.AddUint64(&db.txId, 1)
}

func deleteLessThan[T comparable](s []T, val T) []T {
	for i, v := range s {
		if v == val {
			return s[i:]
		}
	}
	return s
}
