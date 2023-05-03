package database

import (
	"fmt"
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/database/datastruct/dict"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
)

// MutilDB is a set of multiple database set
type MutilDB struct {
	dbSet []*SingleDB
}

// NewDatabase creates a redis database,
func NewMutilDB(opt CouloyDB.Options) *MutilDB {
	mdb := &MutilDB{}

	mdb.dbSet = make([]*SingleDB, 16)
	for i := range mdb.dbSet {
		iOpt := opt
		iOpt.DirPath += fmt.Sprintf("-%03d", i)
		singleDB := NewSingleDB(iOpt)
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *MutilDB) Exec(c *server.Conn, cmdLine [][]byte) (result reply.Reply) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return reply.MakeArgNumErrReply("select")
		}
		return execSelect(c, mdb, cmdLine[1:])
	}
	// normal commands

	dbIndex := c.GetSelectedDB()
	selectedDB := mdb.dbSet[dbIndex]
	return selectedDB.Exec(c, cmdLine)
}

// Close graceful shutdown database
func (mdb *MutilDB) Close() {

}

func execSelect(c *server.Conn, mdb *MutilDB, args [][]byte) reply.Reply {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(mdb.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SetSelectedDB(dbIndex)
	return reply.MakeOkReply()
}

// DataEntity stores data bound to a key, including a string, list, hash, set and so on
type DataEntity struct {
	Data    []byte
	KeyType KeyType
}

// DB stores data and execute user's commands
type SingleDB struct {
	index int
	// key -> DataEntity
	data dict.Dict
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *SingleDB, args [][]byte) reply.Reply

// NewSingleDB create single DB instance
func NewSingleDB(opt CouloyDB.Options) *SingleDB {
	db := &SingleDB{
		data: dict.NewCouloyDict(opt),
	}
	return db
}

// Exec executes command within one database
func (db *SingleDB) Exec(c *server.Conn, cmdLine [][]byte) reply.Reply {

	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fn := cmd.executor
	return fn(db, cmdLine[1:])
}

func (db *SingleDB) Close() {
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
func (db *SingleDB) GetEntity(key string) (*DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	return &DataEntity{Data: raw[1:], KeyType: KeyType(raw[0])}, true
}

// PutEntity a DataEntity into DB
func (db *SingleDB) PutEntity(key string, entity *DataEntity) int {

	return db.data.Put(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// PutIfExists edit an existing DataEntity
func (db *SingleDB) PutIfExists(key string, entity *DataEntity) int {
	return db.data.PutIfExists(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *SingleDB) PutIfAbsent(key string, entity *DataEntity) int {
	return db.data.PutIfAbsent(key, append([]byte{byte(entity.KeyType)}, entity.Data...))
}

// CheckIsExist check the given key from db if exist
func (db *SingleDB) CheckIsExist(key string) bool {
	return db.data.Exist(key)
}

// Remove the given key from db
func (db *SingleDB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *SingleDB) Removes(keys ...string) (deleted int) {
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
func (db *SingleDB) Flush() {
	db.data.Clear()
}
