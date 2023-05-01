package database

import (
	"github.com/Kirov7/CouloyDB/server/resp/reply"
)

// Ping the server
func Ping(db *DB, args [][]byte) reply.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func init() {
	RegisterCommand("ping", Ping, -1)
}
