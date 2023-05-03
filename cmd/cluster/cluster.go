package cluster

import (
	"context"
	"fmt"
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/cmd/root"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/database"
	"github.com/Kirov7/CouloyDB/server/resp"
	"github.com/Kirov7/CouloyDB/server/resp/options"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var configFile string
var cmdPeers, cmdIndexType, cmdDirPath string
var cmdSyncWrites *bool
var cmdMergeInterval, cmdSelf, cmdDataFileSize *int64

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster kuloy",
	Long:  `kuloy's goal is to compromise between performance and storage costs, as an alternative to Redis in some scenarios.`,
	Run: func(cmd *cobra.Command, args []string) {
		if configFile != "" {
			viper.SetConfigFile(configFile)

			err := viper.ReadInConfig()
			if err != nil {
				fmt.Printf("Unable to read configuration file: %s, please check whether the path is correct \n", configFile)
				os.Exit(1)
			}
		} else {
			viper.Set("cluster.peers", strings.Split(cmdPeers, ","))
			viper.Set("cluster.self", *cmdSelf)

			viper.Set("engine.dirPath", cmdDirPath)
			viper.Set("engine.dataFileSize", *cmdDataFileSize)
			viper.Set("engine.indexType", cmdIndexType)
			viper.Set("engine.syncWrites", cmdSyncWrites)
			viper.Set("engine.mergeInterval", *cmdMergeInterval)
		}

		cluster := viper.GetStringSlice("cluster.peers")
		self := viper.GetInt("cluster.self")

		syncWrites := viper.GetBool("engine.syncWrites")
		dirPath := viper.GetString("engine.dirPath")
		dataFileSize := viper.GetInt64("engine.dataFileSize")
		indexType := viper.GetString("engine.indexType")
		mergeInterval := viper.GetInt64("engine.mergeInterval")

		addr := cluster[self]
		_, realPort, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatal("Fail read Port: ", err)
			return
		}
		realAddr := "0.0.0.0" + ":" + realPort

		kuloyOpts := options.KuloyOptions{
			StandaloneOpt: CouloyDB.Options{
				DirPath:       dirPath,
				DataFileSize:  dataFileSize,
				SyncWrites:    syncWrites,
				MergeInterval: mergeInterval,
			},
			Peers: cluster,
			Self:  self,
		}
		switch indexType {
		case "hashmap":
			kuloyOpts.StandaloneOpt.IndexType = meta.Btree
		case "btree":
			kuloyOpts.StandaloneOpt.IndexType = meta.Btree
		case "art":
			kuloyOpts.StandaloneOpt.IndexType = meta.ART
		}

		resp.SetupEngine(kuloyOpts, true)
		kuloyServerStart(realAddr)
	},
}

func kuloyServerStart(addr string) {
	router := server.NewTcpSliceRouter()
	router.Group().Use(
		resp.RespMiddleware(),
	)

	tailFunc := func(c *server.TcpSliceRouterContext) server.TCPHandler {
		return server.NewTailService(c)
	}

	routerHandler := server.NewTcpSliceRouterHandler(tailFunc, router)

	fmt.Println("== tpcSever : addr : ", addr)
	srv := &server.TcpServer{
		Addr:             addr,
		Handler:          routerHandler,
		KeepAliveTimeout: 5 * time.Minute,
		NotifyStarted:    database.JoinClusterFunc,
	}

	go func() {
		log.Printf("kuloy cluster peer server running in %s \n", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != server.ErrServerClosed {
			log.Fatalln(err)
		}
	}()

	quit := make(chan os.Signal)
	// SIGINT User sends INTR character (Ctrl + C) to trigger kill -2
	// End of SIGTERM program (can be captured, blocked, or ignored)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting Down kuloy server...")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Close(ctx); err != nil {
		log.Fatalln("kuloy server shutdown, cause by : ", err)
	}

	select {
	case <-ctx.Done():
		log.Println("wait timeout...")
	}
	log.Println("kuloy server stop success...")
}

func init() {
	clusterCmd.Flags().StringVarP(&configFile, "cpath", "c", "", "Path of the configuration file in yaml, json and toml format (optional)")

	clusterCmd.Flags().StringVarP(&cmdPeers, "peers", "p", "", "All the instances in the cluster ip:port, separated by commas (such as 192.168.1.1:8080,192168:1.2:8080)")
	cmdSelf = clusterCmd.Flags().Int64P("self", "s", 0, "Local Index in the cluster (optional)")

	clusterCmd.Flags().StringVarP(&cmdDirPath, "dpath", "d", "./datafile", "Directory Path where data logs are stored [default at ./datafile]")
	clusterCmd.Flags().StringVarP(&cmdIndexType, "itype", "t", "btree", "Type of memory index (hashmap/btree/art)")
	cmdMergeInterval = clusterCmd.Flags().Int64P("minterval", "", 3600, "merge frequently interval (unit: second) [default 8 hours]")
	cmdDataFileSize = clusterCmd.Flags().Int64P("dfsize", "", 268435456, "Maximum byte size per datafile (unit: Byte) [default 256MB]")
	cmdSyncWrites = clusterCmd.Flags().BoolP("sync", "", false, "Whether to enable write synchronization (true/false)")

	root.AddCommand(clusterCmd)
}
