package CouloyDB

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key can not be empty")
	ErrKeyIsControlChar  = errors.New("the key can not be control char (ASCII 0~31 || 127)")
	ErrUpdateIndexFailed = errors.New("update memTable failed")
	ErrKeyNotFound       = errors.New("the key not found")
	ErrInMerging         = errors.New("process is in merging")
	ErrDirOccupied       = errors.New("db directory is occupied")
)
