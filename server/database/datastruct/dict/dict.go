package dict

// Consumer is used to traversal dict, if it returns false the traversal will be break
type Consumer func(key []byte, val []byte) bool

// Dict is interface of a key-value data structure
type Dict interface {
	Get(key string) (val []byte, exists bool)
	Len() int
	Put(key string, val []byte) (result int)
	PutIfAbsent(key string, val []byte) (result int)
	PutIfExists(key string, val []byte) (result int)
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string
	RandomDistinctKeys(limit int) []string
	Clear()
	Exist(key string) bool
}
