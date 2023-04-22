package server

import "errors"

var (
	ErrServerClosed     = errors.New("tcp: Server closed")
	ErrAbortHandler     = errors.New("tcp: abort TCPHandler")
	ServerContextKey    = "tcp-server"
	LocalAddrContextKey = "local-addr"
)
