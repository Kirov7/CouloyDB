package CouloyDB

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrUpdateIndexFailed = errors.New("update index failed")
	ErrKeyNotFound       = errors.New("the key not found")
)
