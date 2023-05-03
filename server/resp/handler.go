package resp

import (
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/database"
	"github.com/Kirov7/CouloyDB/server/resp/options"
	"github.com/Kirov7/CouloyDB/server/resp/parser"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

var respHandler *RespHandler

// RespHandler implements tcp.Handler and serves as a redis handler
type RespHandler struct {
	activeConn sync.Map // *client -> placeholder
	db         database.Database
	inShutdown int32 // refusing new client and new request
}

// SetupEngine creates a RespHandler instance
func SetupEngine(opt options.KuloyOptions, isCluster bool) {
	respHandler = &RespHandler{}
	if isCluster {
		respHandler.db = database.NewClusterDB(opt)
	} else {
		respHandler.db = database.NewMutilDB(opt.StandaloneOpt)
	}
}

func (h *RespHandler) closeConn(conn *net.Conn) {
	_ = (*conn).Close()
	h.activeConn.Delete(conn)
}

// Close stops handler
func (h *RespHandler) Close() error {
	log.Println("handler shutting down...")
	atomic.AddInt32(&h.inShutdown, 1)

	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		conn := key.(*net.Conn)
		_ = (*conn).Close()
		return true
	})
	h.db.Close()
	return nil
}

// parse Resp and use db exec the request if this node is not responsible for the request, then Forward the request(store the useful node in the context)
func RespMiddleware() func(c *server.TcpSliceRouterContext) {
	return func(ctx *server.TcpSliceRouterContext) {

		conn := ctx.GetConn()
		respHandler.activeConn.Store(&conn, struct{}{})
		ch := parser.ParseStream(conn)
		for payload := range ch {
			if payload.Err != nil {
				if payload.Err == io.EOF ||
					payload.Err == io.ErrUnexpectedEOF ||
					strings.Contains(payload.Err.Error(), "use of closed network connection") {
					// connection closed
					log.Println("connection closed: " + ctx.GetString(server.RemoteAddrContextKey))
					respHandler.closeConn(&conn)
					ctx.Abort()
					return
				}
				// protocol err
				errReply := reply.MakeErrReply(payload.Err.Error())
				err := ctx.Write(errReply.ToBytes())
				if err != nil {
					log.Println("connection closed: " + ctx.GetString(server.RemoteAddrContextKey))
					respHandler.closeConn(&conn)
					ctx.Abort()
					return
				}
				continue
			}
			if payload.Data == nil {
				log.Println("empty payload")
				continue
			}
			r, ok := payload.Data.(*reply.MultiBulkReply)
			if !ok {
				log.Println("require multi bulk reply")
				continue
			}

			result := respHandler.db.Exec(ctx.GetClientConn(), r.Args)
			if result != nil {
				_ = ctx.Write(result.ToBytes())
			} else {
				_ = ctx.Write(unknownErrReplyBytes)
			}
		}
		ctx.Abort()
	}
}
