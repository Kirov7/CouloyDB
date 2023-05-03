package database

import (
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
)

func init() {
	RegisterCommand("ping", execPing, execPingCluster, -1)
}

// Ping the server
func execPing(db *SingleDB, args [][]byte) reply.Reply {
	if len(args) == 0 {
		return &reply.PongReply{}
	} else if len(args) == 1 {
		return reply.MakeStatusReply(string(args[0]))
	} else {
		return reply.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

func execPingCluster(cluster *ClusterDatabase, c *server.Conn, cmdAndArgs [][]byte) reply.Reply {
	return cluster.db.Exec(c, cmdAndArgs)
}
