package server

import (
	"context"
	"net"
)

func NewTailService(c *TcpSliceRouterContext) *TailService {
	return func() *TailService {
		return &TailService{ctx: c.Ctx}
	}()
}

type TailService struct {
	ctx context.Context
}

func (t TailService) ServeTCP(ctx context.Context, conn net.Conn) {
	// do what when resp parser has down
}
