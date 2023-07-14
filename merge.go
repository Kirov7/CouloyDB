package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

func (db *DB) Merge() error {
	db.mergeChan <- struct{}{}
	return <-db.mergeDone
}

// Merge Clear invalid data and generate the hint file
func (db *DB) merge() error {
	if db.activityFile == nil {
		return nil
	}

	db.mu.Lock()

	if db.isMerging {
		db.mu.Unlock()
		return public.ErrInMerging
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err := db.activityFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	// convert current activityFile to oldFile
	db.oldFile[db.activityFile.FileId] = db.activityFile
	// open a new activityFile
	if err := db.setActivityFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	// record recent not merge file id
	nowMergeFile := db.activityFile.FileId

	var mergeFiles []*data.DataFile
	for _, file := range db.oldFile {
		mergeFiles = append(mergeFiles, file)
	}

	db.mu.Unlock()

	// merge from small to big
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()

	// if the path is already exist, then remove the path
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// create a new db instance temporarily
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	// in merging sync is not needed because it will not affect user's normal behavior If merge panic occurs
	mergeOptions.SyncWrites = false
	mergeDb, err := NewCouloyDB(mergeOptions)
	if err != nil {
		return err
	}

	// open a hintFile, store the index
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// iterate every dataFile and process them
	for _, oldFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := oldFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// parse and get the real key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			var logRecordPos *data.LogPos
			switch logRecord.DSType {
			case data.String:
				logRecordPos = db.strIndex.Get(realKey)
			case data.Hash:
				realKey, field := decodeFieldKey(realKey)
				logRecordPos = db.hashIndex[string(realKey)].Get(field)
			}
			// compare with the memTable, if the already exist in memTable then rewrite it
			if logRecordPos != nil && logRecordPos.Fid == oldFile.FileId && logRecordPos.Offset == offset {
				// clean the txId mark
				logRecord.Key = encodeKeyWithTxId(realKey, public.NO_TX_ID)
				pos, err := mergeDb.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				// write the pos to the hint file
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// add offset
			offset += size
		}
	}

	// sync the file
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDb.Sync(); err != nil {
		return err
	}

	// add a file to mark merge is finish
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   public.MERGE_FIN_Key,
		Value: []byte(strconv.Itoa(int(nowMergeFile))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return err
}

// create a new directory
func (db *DB) getMergePath() string {
	// get the parent dir
	dir := path.Dir(path.Clean(db.options.DirPath))
	// get the current dir name
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+public.MergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	dirEnts, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// find the mark and check whether the merger is complete
	var MergeFin bool
	// stores the names of all merge files temporarily
	var mergeFileNames []string

	for _, ent := range dirEnts {
		if ent.Name() == public.MergeFinishedFileName {
			MergeFin = true
		}
		mergeFileNames = append(mergeFileNames, ent.Name())
	}

	// if the merge does not complete, return directly
	if !MergeFin {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	// remove the old dataFile
	var fileId uint32
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err != nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// move the new dataFile to data Dir
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		dstPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	// check hint file is exist
	hintFileName := filepath.Join(db.options.DirPath, public.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	// open the hint file
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	// read the index from the file
	var offset int64
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// get the real pos index
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.strIndex.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
