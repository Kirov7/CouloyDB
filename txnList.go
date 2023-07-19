package CouloyDB

func (txn *Txn) LPush(key []byte, values [][]byte) error {
	return nil
}

func (txn *Txn) RPush(key []byte, values [][]byte) error {
	return nil
}

func (txn *Txn) LPop(key []byte) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) RPop(key []byte) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) LLen(key []byte) (int, error) {
	return 0, nil
}

func (txn *Txn) LIndex(key []byte, index int) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) LSet(key []byte, index int, value []byte) error {
	return nil
}

func (txn *Txn) LRem(key []byte, index int) error {
	return nil
}

func (txn *Txn) LRange(key []byte, start, stop int) ([][]byte, error) {
	return nil, nil
}

func (txn *Txn) LTrim(key []byte, start, stop int) error {
	return nil
}
