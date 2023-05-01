package database

import (
	"strings"
)

var cmdTable = make(map[string]*command)

type command struct {
	executor ExecFunc
	arity    int // allow number of args, arity < 0 means len(args) >= -arity
}

// RegisterCommand registers a new command
// arity means allowed number of cmdArgs, arity < 0 means len(args) >= -arity.
// for example: the arity of `get` is 2, `mget` is -2
func RegisterCommand(name string, executor ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: executor,
		arity:    arity,
	}
}
