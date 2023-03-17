package CouloyDB

import (
	"errors"
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/meta"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options      Options
	activityFile *data.DataFile
	oldFile      map[uint32]*data.DataFile
	index        meta.MemIndex
	lock         *sync.RWMutex
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

	// Init DB
	db := &DB{
		options: opt,
		oldFile: make(map[uint32]*data.DataFile),
		index:   meta.NewIndexer(opt.IndexerType),
		lock:    new(sync.RWMutex),
	}

	// Load DataFile and index
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	if ok := db.index.Put(key, pos); !ok {
		return ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotFound
	}
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

func (db *DB) Del(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// Check if exist in memory index
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// Build deleted tags LogRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	// Delete key in memory index
	if ok := db.index.Del(key); !ok {
		return ErrUpdateIndexFailed
	}
	return nil
}

func (db *DB) appendLogRecord(log *data.LogRecord) (*data.LogRecordPos, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

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
	pos := &data.LogRecordPos{
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

	if err := db.loadIndex(fileIds); err != nil {
		return err
	}
	return nil
}

func (db *DB) loadIndex(fids []int) error {
	if len(fids) == 0 {
		return nil
	}
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
			logRecordPos := &data.LogRecordPos{Fid: fileId, Offset: offset}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Del(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
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
