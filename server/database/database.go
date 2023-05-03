package database

import (
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// Database is the interface for redis style storage engine
type Database interface {
	Exec(c *server.Conn, args [][]byte) reply.Reply
	Close()
}

type KeyType byte

const (
	STRING_TYPE KeyType = iota
	LIST_TYPE
	HASH_TYPE
	SET_TYPE
	SORTSET_TYPE
	BITMAP_TYPE
)
