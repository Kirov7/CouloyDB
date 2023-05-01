package server

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"time"
)

type Conn struct {
	server    *TcpServer
	cancelCtx context.CancelFunc
	// Raw Connection
	rwc net.Conn
	// Record remote ip
	KuloyClient
}

func (s *TcpServer) newConn(rwc net.Conn) *Conn {
	c := &Conn{
		server: s,
		rwc:    rwc,
	}
	if d := c.server.ReadTimeout; d != 0 {
		c.rwc.SetReadDeadline(time.Now().Add(d))
	}
	if d := c.server.WriteTimeout; d != 0 {
		c.rwc.SetWriteDeadline(time.Now().Add(d))
	}
	if d := c.server.KeepAliveTimeout; d != 0 {
		if tcpConn, ok := c.rwc.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(d)
		}
	}
	return c
}

func (c *Conn) Close() {
	c.rwc.Close()
}

func (c *Conn) serve(ctx context.Context) {
	defer func() {
		if err := recover(); err != nil && err != ErrAbortHandler {
			const size = 64 << 10
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			fmt.Printf("tcp: panic serving %v: %v\n%s", c.remoteAddr, err, buf)
		}
		c.Close()
	}()
	c.remoteAddr = c.rwc.RemoteAddr().String()
	ctx = context.WithValue(ctx, ConnContextKey, c)
	if c.server.Handler == nil {
		panic("handler empty")
	}
	c.server.Handler.ServeTCP(ctx, c.rwc)
}

type KuloyClient struct {
	remoteAddr string
	selectedDB int
}

func (kc *KuloyClient) GetSelectedDB() int {
	return kc.selectedDB
}

func (kc *KuloyClient) SetSelectedDB(dbNum int) {
	kc.selectedDB = dbNum
}
