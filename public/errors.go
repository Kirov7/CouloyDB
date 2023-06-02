package public

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key can not be empty")
	ErrKeyIsControlChar       = errors.New("the key can not be control char (ASCII 0~31 || 127)")
	ErrUpdateIndexFailed      = errors.New("update memTable failed")
	ErrKeyNotFound            = errors.New("the key not found")
	ErrInMerging              = errors.New("process is in merging")
	ErrDirOccupied            = errors.New("db directory is occupied")
	ErrInvalidCRC             = errors.New("invalid crc value, logRecord maybe corrupted")
	ErrLuaInterpreterDisabled = errors.New("the lua Interpreter is not started, can not support execute lua script")
	ErrTransactionConflict    = errors.New("transaction concurrency conflict, please try again")
	ErrHeapEmpty              = errors.New("heap is empty")
	ErrTxnFunctionEmpty       = errors.New("the txn function is empty")
)
