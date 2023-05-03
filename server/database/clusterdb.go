package database

import (
	"context"
	"fmt"
	"github.com/Kirov7/CouloyDB/public/utils/consistent"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp/client"
	"github.com/Kirov7/CouloyDB/server/resp/options"
	"github.com/Kirov7/CouloyDB/server/resp/reply"
	"github.com/hashicorp/memberlist"
	pool "github.com/jolestar/go-commons-pool/v2"
	"github.com/pkg/errors"
	"log"
	"net"
	"runtime/debug"
	"strings"
)

// ClusterDatabase represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type ClusterDatabase struct {
	self string

	members        *memberlist.Memberlist
	consistent     *consistent.Consistent
	peerConnection map[string]*pool.ObjectPool
	db             Database
}

// MakeClusterDatabase creates and starts a node of cluster
func NewClusterDB(opt options.KuloyOptions) *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: opt.Peers[opt.Self],

		db:             NewSingleDB(opt.StandaloneOpt),
		consistent:     consistent.New(),
		peerConnection: make(map[string]*pool.ObjectPool),
	}

	conf := memberlist.DefaultWANConfig()
	conf.Name = cluster.self
	conf.BindAddr = getIP(cluster.self)
	conf.Events = cluster
	//conf.LogOutput = ioutil.Discard

	list, err := memberlist.Create(conf)
	if err != nil {
		log.Fatal("Creat Kuloy gossip memberlist ERROR: ", err)
	}
	cluster.members = list

	cluster.consistent.Set(opt.Peers)
	//ctx := context.Background()
	//for _, peer := range opt.Peers {
	//	cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &client.ConnectionFactory{
	//		Peer: peer,
	//	})
	//}

	JoinClusterFunc = func() {
		ips := getIPs(opt.Peers...)
		_, err = cluster.members.Join(ips)
		if err != nil {
			log.Fatal("Join Kuloy cluster ERROR: ", err)
		}
		nodeList := cluster.members.Members()
		fmt.Printf("== join nodeList: \n")

		for _, node := range nodeList {
			fmt.Println("== node", node.Name, " ", node.Addr)
			if _, ok := cluster.peerConnection[node.Name]; !ok {
				cluster.peerConnection[node.Name] = pool.NewObjectPoolWithDefaultConfig(context.Background(), &client.ConnectionFactory{
					Peer: node.Name,
				})
			}
		}
	}
	return cluster
}

// ClusterExecFunc represents the handler of a redis command
type ClusterExecFunc func(cluster *ClusterDatabase, c *server.Conn, cmdAndArgs [][]byte) reply.Reply

// Close stops current node of cluster
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

// Exec executes command on cluster
func (cluster *ClusterDatabase) Exec(c *server.Conn, cmdLine [][]byte) (result reply.Reply) {
	defer func() {
		if err := recover(); err != nil {
			log.Println(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	if cmd.cExecutor == nil {
		return reply.MakeErrReply("ERR command '" + cmdName + "' not supported in cluster mode")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	fn := cmd.cExecutor
	return fn(cluster, c, cmdLine)
}

// relay command to responsible peer, and return its reply to client
func defaultFunc(cluster *ClusterDatabase, c *server.Conn, args [][]byte) reply.Reply {
	key := string(args[1])
	peer, _ := cluster.consistent.Get(key)
	//fmt.Println("key: ", key, "  ->  peer: ", peer)
	return cluster.relay(peer, c, args)
}

func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	//fmt.Println("peers: ")
	//for k, _ := range cluster.peerConnection {
	//	fmt.Println(k)
	//}
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cluster *ClusterDatabase) relay(peer string, c *server.Conn, args [][]byte) reply.Reply {
	if peer == cluster.self {
		// to self db
		fmt.Println("relay to self")
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	return peerClient.Send(args)
}

// broadcast broadcasts command to all node in cluster
func (cluster *ClusterDatabase) broadcast(c *server.Conn, args [][]byte) map[string]reply.Reply {
	result := make(map[string]reply.Reply)
	for _, node := range cluster.consistent.Members() {
		rep := cluster.relay(node, c, args)
		result[node] = rep
	}
	return result
}

type ClusterEventHandler struct{}

func (cluster *ClusterDatabase) NotifyJoin(n *memberlist.Node) {
	//fmt.Println("joined a new node: ", n.Name, "  ", n.Addr)
	cluster.consistent.Add(n.Name)
	if _, ok := cluster.peerConnection[n.Name]; !ok {
		cluster.peerConnection[n.Name] = pool.NewObjectPoolWithDefaultConfig(context.Background(), &client.ConnectionFactory{Peer: n.Name})
	}
}

func (cluster *ClusterDatabase) NotifyLeave(n *memberlist.Node) {
	//fmt.Println("leaved a node: ", n.Name, "  ", n.Addr)
	cluster.consistent.Remove(n.Name)
	delete(cluster.peerConnection, n.Name)
}

func (cluster *ClusterDatabase) NotifyUpdate(n *memberlist.Node) {}

var JoinClusterFunc func()

func getIPs(addrs ...string) []string {
	var ips []string
	for _, addr := range addrs {
		ip, _, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatal("The format of addr is incorrect: ", err)
		}
		ips = append(ips, ip)
	}
	return ips
}

func getIP(addr string) string {
	ip, _, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatal("The format of addr is incorrect: ", err)
	}
	return ip
}
