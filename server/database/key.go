package database

import (
	"github.com/Kirov7/CouloyDB/public/utils/wildcard"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
)

func init() {
	RegisterCommand("Del", execDel, execDelCluster, -2)
	RegisterCommand("Exists", execExists, defaultFunc, -2)
	RegisterCommand("Keys", execKeys, nil, 2)
	RegisterCommand("FlushDB", execFlushDB, execFlushDBCluster, -1)
	RegisterCommand("Type", execType, defaultFunc, 2)
	RegisterCommand("Rename", execRename, execRenameCluster, 3)
	RegisterCommand("RenameNx", execRenameNx, execRenameCluster, 3)
}

// execDel removes a key from db
func execDel(db *SingleDB, args [][]byte) reply.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}

	deleted := db.Removes(keys...)
	return reply.MakeIntReply(int64(deleted))
}

// execExists checks if a is existed in db
func execExists(db *SingleDB, args [][]byte) reply.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		if db.CheckIsExist(key) {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

// execFlushDB removes all data in current db
func execFlushDB(db *SingleDB, args [][]byte) reply.Reply {
	db.Flush()
	return &reply.OkReply{}
}

// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.KeyType {
	case STRING_TYPE:
		return reply.MakeStatusReply("string")
	case LIST_TYPE:
		return reply.MakeStatusReply("list")
	case HASH_TYPE:
		return reply.MakeStatusReply("hash")
	case SET_TYPE:
		return reply.MakeStatusReply("set")
	case SORTSET_TYPE:
		return reply.MakeStatusReply("zset")
	case BITMAP_TYPE:
		return reply.MakeStatusReply("bitmap")
	}
	return &reply.UnknownErrReply{}
}

// execRename a key
func execRename(db *SingleDB, args [][]byte) reply.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity)
	db.Remove(src)
	return &reply.OkReply{}
}

// execRenameNx a key, only if the new key does not exist
func execRenameNx(db *SingleDB, args [][]byte) reply.Reply {
	src := string(args[0])
	dest := string(args[1])

	if db.CheckIsExist(dest) {
		return reply.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.Removes(src, dest) // clean src and dest with their ttl
	db.PutEntity(dest, entity)
	return reply.MakeIntReply(1)
}

// execKeys returns all keys matching the given pattern
func execKeys(db *SingleDB, args [][]byte) reply.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key []byte, val []byte) bool {
		if pattern.IsMatch(string(key)) {
			result = append(result, key)
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

// execDelCluster atomically removes given writeKeys from cluster, writeKeys can be distributed on any node
// if the given writeKeys are distributed on different node, Del will use try-commit-catch to remove them
func execDelCluster(cluster *ClusterDatabase, c *server.Conn, args [][]byte) reply.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
		intReply, ok := v.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}

// execFlushDBCluster removes all data in current database
func execFlushDBCluster(cluster *ClusterDatabase, c *server.Conn, args [][]byte) reply.Reply {
	replies := cluster.broadcast(c, args)
	var errReply reply.ErrorReply
	for _, v := range replies {
		if reply.IsErrorReply(v) {
			errReply = v.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return &reply.OkReply{}
	}
	return reply.MakeErrReply("error occurs: " + errReply.Error())
}

// execRenameCluster renames a key, the origin and the destination must within the same node
func execRenameCluster(cluster *ClusterDatabase, c *server.Conn, args [][]byte) reply.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer, _ := cluster.consistent.Get(src)
	destPeer, _ := cluster.consistent.Get(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.relay(srcPeer, c, args)
}

// execKeysCluster renames a key, the origin and the destination must within the same node
func execKeysCluster(cluster *ClusterDatabase, c *server.Conn, args [][]byte) reply.Reply {
	if len(args) != 3 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[1])
	dest := string(args[2])

	srcPeer, _ := cluster.consistent.Get(src)
	destPeer, _ := cluster.consistent.Get(dest)

	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within one slot in cluster mode")
	}
	return cluster.relay(srcPeer, c, args)
}
