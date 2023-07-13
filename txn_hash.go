package CouloyDB

func (txn *Txn) HSet(key, filed, value []byte) error {
	return nil
}

func (txn *Txn) HGet(key, filed []byte) ([]byte, error) {
	return nil, nil
}

func (txn *Txn) HDel(key, filed []byte) error {
	return nil
}

func (txn *Txn) HExist(key, filed []byte) bool {
	return false
}

func (txn *Txn) HGetAll(key []byte) ([][]byte, [][]byte, error) {
	return nil, nil, nil
}

func (txn *Txn) HLen(key []byte) int {
	return 0
}
