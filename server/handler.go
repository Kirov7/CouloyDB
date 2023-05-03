package server

import (
	"context"
	"net"
)

// HandleFunc represents application handler function
type HandleFunc func(ctx context.Context, conn net.Conn)

// TCPHandler represents application handler over tcp
type TCPHandler interface {
	ServeTCP(ctx context.Context, conn net.Conn)
}
