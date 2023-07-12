package CouloyDB

import (
	"github.com/Kirov7/CouloyDB/data"
	"github.com/Kirov7/CouloyDB/public"
)

func (f *Facade) Set(key, value []byte) error {
	return f.db.Put(key, value)
}

func (f *Facade) Get(key []byte) ([]byte, error) {
	return f.db.Get(key)
}

func (f *Facade) Del(key []byte) error {
	return f.db.Del(key)
}

func (f *Facade) SetNX(key, value []byte) error {
	if err := checkKey(key); err != nil {
		return err
	}

	f.db.getIndexLockByType(String).Lock()
	defer f.db.getIndexLockByType(String).Unlock()

	if dataPos := f.db.memTable.Get(key); dataPos != nil {
		return public.ErrKeyExist
	}

	logRecord := &data.LogRecord{
		Key:   encodeKeyWithTxId(key, public.NO_TX_ID),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := f.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	f.db.Notify(string(key), value, PutEvent)

	if ok := f.db.memTable.Put(key, pos); !ok {
		return public.ErrUpdateIndexFailed
	}
	return nil
}

func (f *Facade) GetSet(key, value []byte) ([]byte, error) {
	f.db.getIndexLockByType(String).Lock()
	defer f.db.getIndexLockByType(String).Unlock()

	var (
		oldVal []byte
		err    error
	)

	if err = checkKey(key); err != nil {
		return nil, err
	}

	pos := f.db.memTable.Get(key)
	if pos != nil {
		oldVal, err = f.db.getValueByPos(pos)
		if err != nil {
			return nil, err
		}
	}

	logRecord := &data.LogRecord{
		Key:   encodeKeyWithTxId(key, public.NO_TX_ID),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err = f.db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil, err
	}

	f.db.Notify(string(key), value, PutEvent)

	if ok := f.db.memTable.Put(key, pos); !ok {
		return nil, public.ErrUpdateIndexFailed
	}
	return oldVal, nil
}

func (f *Facade) StrLen(key []byte) (int, error) {
	f.db.getIndexLockByType(String).RLock()
	defer f.db.getIndexLockByType(String).RUnlock()

	pos := f.db.memTable.Get(key)
	if pos != nil {
		value, err := f.db.getValueByPos(pos)
		if err != nil {
			return 0, err
		}
		return len(value), nil
	}
	return 0, public.ErrKeyNotFound
}
