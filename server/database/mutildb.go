package database

import (
	"fmt"
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
	"log"
	"runtime/debug"
	"strconv"
	"strings"
)

// Database is a set of multiple database set
type Database struct {
	dbSet []*DB
}

// NewDatabase creates a redis database,
func NewDatabase(opt CouloyDB.Options) *Database {
	mdb := &Database{}

	mdb.dbSet = make([]*DB, 16)
	for i := range mdb.dbSet {
		iOpt := opt
		iOpt.DirPath += fmt.Sprintf("-%03d", i)
		singleDB := NewDB(iOpt)
		singleDB.index = i
		mdb.dbSet[i] = singleDB
	}
	return mdb
}

// Exec executes command
// parameter `cmdLine` contains command and its arguments, for example: "set key value"
func (mdb *Database) Exec(c *server.Conn, cmdLine [][]byte) (result reply.Reply) {
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
func (mdb *Database) Close() {

}

func execSelect(c *server.Conn, mdb *Database, args [][]byte) reply.Reply {
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
