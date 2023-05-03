package server

import (
	"context"
	"math"
	"net"
)

const abortIndex int8 = math.MaxInt8 / 2

type TcpHandlerFunc func(*TcpSliceRouterContext)

type TcpSliceRouter struct {
	group *TcpSliceGroup
}

type TcpSliceGroup struct {
	*TcpSliceRouter
	path     string
	handlers []TcpHandlerFunc
}

type TcpSliceRouterContext struct {
	conn net.Conn
	Ctx  context.Context
	*TcpSliceGroup
	index int8
}

func newTcpSliceRouterContext(conn net.Conn, r *TcpSliceRouter, ctx context.Context) *TcpSliceRouterContext {
	c := &TcpSliceRouterContext{conn: conn, TcpSliceGroup: r.group, Ctx: ctx}
	c.Reset()
	return c
}

func (c *TcpSliceRouterContext) GetConn() net.Conn {
	return c.conn
}

func (c *TcpSliceRouterContext) GetClientConn() *Conn {
	return c.Ctx.Value(ConnContextKey).(*Conn)
}

func (c *TcpSliceRouterContext) Write(bytes []byte) error {
	_, err := c.conn.Write(bytes)
	return err
}

func (c *TcpSliceRouterContext) Read(key interface{}) interface{} {
	return c.Ctx.Value(key)
}

func (c *TcpSliceRouterContext) Get(key interface{}) interface{} {
	return c.Ctx.Value(key)
}

func (c *TcpSliceRouterContext) Set(key, val interface{}) {
	c.Ctx = context.WithValue(c.Ctx, key, val)
}

func (c *TcpSliceRouterContext) GetString(key interface{}) string {
	return c.Ctx.Value(key).(string)
}

type TcpSliceRouterHandler struct {
	coreFunc func(*TcpSliceRouterContext) TCPHandler
	router   *TcpSliceRouter
}

func (w *TcpSliceRouterHandler) ServeTCP(ctx context.Context, conn net.Conn) {
	c := newTcpSliceRouterContext(conn, w.router, ctx)
	c.handlers = append(c.handlers, func(c *TcpSliceRouterContext) {
		w.coreFunc(c).ServeTCP(ctx, conn)
	})
	c.Reset()
	c.Next()
}

func NewTcpSliceRouterHandler(coreFunc func(*TcpSliceRouterContext) TCPHandler, router *TcpSliceRouter) *TcpSliceRouterHandler {
	return &TcpSliceRouterHandler{
		coreFunc: coreFunc,
		router:   router,
	}
}

func NewTcpSliceRouter() *TcpSliceRouter {
	return &TcpSliceRouter{}
}

func (g *TcpSliceRouter) Group() *TcpSliceGroup {
	return &TcpSliceGroup{
		TcpSliceRouter: g,
	}
}

func (g *TcpSliceGroup) Use(middlewares ...TcpHandlerFunc) *TcpSliceGroup {
	g.handlers = append(g.handlers, middlewares...)
	g.TcpSliceRouter.group = g
	return g
}

func (c *TcpSliceRouterContext) Next() {
	c.index++
	for c.index < int8(len(c.handlers)) {
		c.handlers[c.index](c)
		c.index++
	}
}

func (c *TcpSliceRouterContext) Abort() {
	c.index = abortIndex
}

func (c *TcpSliceRouterContext) IsAborted() bool {
	return c.index >= abortIndex
}

func (c *TcpSliceRouterContext) Reset() {
	c.index = -1
}
