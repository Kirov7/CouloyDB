package server

import (
	"context"
	"io"
	"log"
	"net"
	"time"
)

func NewTcpReverseProxy(c *TcpSliceRouterContext) *TcpReverseProxy {
	return func() *TcpReverseProxy {
		addr := c.GetString(ForwardAddrContextKey)
		return &TcpReverseProxy{
			ctx:             c.Ctx,
			Addr:            addr,
			KeepAlivePeriod: time.Second,
			DialTimeout:     time.Second,
		}
	}()
}

// TcpReverseProxy Used to forward requests that cannot be processed
type TcpReverseProxy struct {
	ctx                  context.Context // Each request is set separately
	Addr                 string
	KeepAlivePeriod      time.Duration
	DialTimeout          time.Duration
	DialContext          func(ctx context.Context, network, address string) (net.Conn, error)
	OnDialError          func(src net.Conn, dstDialErr error)
	ProxyProtocolVersion int
}

func (trp *TcpReverseProxy) dialTimeout() time.Duration {
	if trp.DialTimeout > 0 {
		return trp.DialTimeout
	}
	return 10 * time.Second
}

func (trp *TcpReverseProxy) dialContext() func(ctx context.Context, network, address string) (net.Conn, error) {
	if trp.DialContext != nil {
		return trp.DialContext
	}
	return (&net.Dialer{
		Timeout:   trp.DialTimeout,
		KeepAlive: trp.KeepAlivePeriod,
	}).DialContext
}

func (trp *TcpReverseProxy) keepAlivePeriod() time.Duration {
	if trp.KeepAlivePeriod != 0 {
		return trp.KeepAlivePeriod
	}
	return time.Minute
}

// ServeTCP Pass in the upstream connection, where the downstream connection and data exchange are completed
func (trp *TcpReverseProxy) ServeTCP(ctx context.Context, src net.Conn) {
	var cancel context.CancelFunc
	if trp.DialTimeout >= 0 {
		ctx, cancel = context.WithTimeout(ctx, trp.dialTimeout())
	}
	dst, err := trp.dialContext()(ctx, "tcp", trp.Addr)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		trp.onDialError()(src, err)
		return
	}

	defer func() { go dst.Close() }() // Remember to exit the downstream connection

	if ka := trp.keepAlivePeriod(); ka > 0 {
		if c, ok := dst.(*net.TCPConn); ok {
			c.SetKeepAlive(true)
			c.SetKeepAlivePeriod(ka)
		}
	}
	errc := make(chan error, 1)
	go trp.proxyCopy(errc, src, dst)
	go trp.proxyCopy(errc, dst, src)
	<-errc
}

func (trp *TcpReverseProxy) onDialError() func(src net.Conn, dstDialErr error) {
	if trp.OnDialError != nil {
		return trp.OnDialError
	}
	return func(src net.Conn, dstDialErr error) {
		log.Printf("tcpproxy: for incoming conn %v, error dialing %q: %v", src.RemoteAddr().String(), trp.Addr, dstDialErr)
		src.Close()
	}
}

func (trp *TcpReverseProxy) proxyCopy(errc chan<- error, dst, src net.Conn) {
	_, err := io.Copy(dst, src)
	errc <- err
}
