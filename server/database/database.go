package database

import (
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/database/datastruct/dict"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
	"strings"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

//// Database is the interface for redis style storage engine
//type Database interface {
//	Exec(client net.Conn, args [][]byte) reply.Reply
//	Close()
//}

type KeyType byte

const (
	STRING_TYPE KeyType = iota
	LIST_TYPE
	HASH_TYPE
	SET_TYPE
	SORTSET_TYPE
	BITMAP_TYPE
)

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct {
	Data    []byte
	KeyType KeyType
}

// DB stores data and execute user's commands
type DB struct {
	index int
	// key -> DataEntity
	data dict.Dict
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) reply.Reply

// NewDB create DB instance
func NewDB(opt CouloyDB.Options) *DB {
	db := &DB{
		data: dict.NewCouloyDict(opt),
	}
	return db
}

// Exec executes command within one database
func (db *DB) Exec(c *server.Conn, cmdLine [][]byte) reply.Reply {

	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	return fun(db, cmdLine[1:])
}

func (db *DB) Close() {
	db.index = 0
}

func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- data Access ----- */

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	return &DataEntity{Data: raw[1:], KeyType: KeyType(raw[0])}, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *DataEntity) int {

	return db.data.Put(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *DataEntity) int {
	return db.data.PutIfExists(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *DataEntity) int {
	return db.data.PutIfAbsent(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// CheckIsExist check the given key from db if exist
func (db *DB) CheckIsExist(key string) bool {
	return db.data.Exist(key)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		if db.data.Exist(key) {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
