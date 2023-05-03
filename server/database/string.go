package database

import (
	"fmt"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
)

func init() {
	RegisterCommand("Get", execGet, defaultFunc, 2)
	RegisterCommand("Set", execSet, defaultFunc, -3)
	RegisterCommand("SetNx", execSetNX, defaultFunc, 3)
	RegisterCommand("GetSet", execGetSet, defaultFunc, 3)
	RegisterCommand("StrLen", execStrLen, defaultFunc, 2)
}

func (db *SingleDB) getAsString(key string) ([]byte, reply.ErrorReply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, nil
	}
	return entity.Data, nil
}

// execGet returns string value bound to the given key
func execGet(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	fmt.Println("收到get请求, key: ", key)
	bytes, err := db.getAsString(key)
	if err != nil {
		return err
	}
	if bytes == nil {
		return &reply.NullBulkReply{}
	}
	return reply.MakeBulkReply(bytes)
}

// execSet sets string value and time to live to the given key
func execSet(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	value := args[1]
	fmt.Println("收到set请求, key: ", key, " value: ", value)
	entity := &DataEntity{
		Data:    value,
		KeyType: STRING_TYPE,
	}
	db.PutEntity(key, entity)
	return &reply.OkReply{}
}

// execSetNX sets string if not exists
func execSetNX(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	value := args[1]
	entity := &DataEntity{
		Data:    value,
		KeyType: STRING_TYPE,
	}
	result := db.PutIfAbsent(key, entity)
	return reply.MakeIntReply(int64(result))
}

// execGetSet sets value of a string-type key and returns its old value
func execGetSet(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	value := args[1]

	entity, exists := db.GetEntity(key)
	db.PutEntity(key, &DataEntity{Data: value, KeyType: STRING_TYPE})
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data
	return reply.MakeBulkReply(old)
}

// execStrLen returns len of string value bound to the given key
func execStrLen(db *SingleDB, args [][]byte) reply.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeNullBulkReply()
	}
	old := entity.Data
	return reply.MakeIntReply(int64(len(old)))
}
