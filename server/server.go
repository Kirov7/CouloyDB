package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type TcpServer struct {
	Addr    string
	Handler TCPHandler
	err     error
	ctx     context.Context

	WriteTimeout     time.Duration
	ReadTimeout      time.Duration
	KeepAliveTimeout time.Duration

	mu         sync.Mutex
	inShutdown int32
	doneChan   chan struct{}
	l          *onceCloseListener

	NotifyStarted func()
}

type onceCloseListener struct {
	net.Listener
	once     sync.Once
	closeErr error
}

func (oc *onceCloseListener) Close() error {
	oc.once.Do(func() {
		oc.closeErr = oc.Listener.Close()
	})
	return oc.closeErr
}

func (s *TcpServer) shuttingDown() bool {
	return atomic.LoadInt32(&s.inShutdown) != 0
}

func ListenAndServe(addr string, handler TCPHandler) error {
	server := &TcpServer{Addr: addr, Handler: handler, doneChan: make(chan struct{})}
	return server.ListenAndServe()
}

func (s *TcpServer) ListenAndServe() error {

	if s.shuttingDown() {
		return ErrServerClosed
	}
	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	addr := s.Addr
	if addr == "" {
		return errors.New("need addr")
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(listener)
}

func (s *TcpServer) Close(ctx context.Context) error {
	atomic.StoreInt32(&s.inShutdown, 1)
	close(s.doneChan)
	return s.l.Close()
}

func (s *TcpServer) Serve(l net.Listener) error {
	s.l = &onceCloseListener{Listener: l}
	defer s.l.Close()
	if s.ctx == nil {
		s.ctx = context.Background()
	}
	baseCtx := s.ctx
	ctx := context.WithValue(baseCtx, ServerContextKey, s)

	if s.NotifyStarted != nil {
		go s.NotifyStarted()
	}
	fmt.Printf("== 开始tcp服务\n")
	for {
		rw, e := l.Accept()
		if e != nil {
			select {
			case <-s.getDoneChan():
				return ErrServerClosed
			default:
			}
			fmt.Printf("accept fail, err: %v\n", e)
			continue
		}
		fmt.Printf("get new conn: %s", rw.RemoteAddr())
		go s.newConn(rw).serve(ctx)
	}
}

func (s *TcpServer) getDoneChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.doneChan == nil {
		s.doneChan = make(chan struct{})
	}
	return s.doneChan
}
