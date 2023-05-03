package standalone

import (
	"context"
	"fmt"
	"github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/cmd/root"
	"github.com/Kirov7/CouloyDB/meta"
	"github.com/Kirov7/CouloyDB/server"
	"github.com/Kirov7/CouloyDB/server/resp"
	"github.com/Kirov7/CouloyDB/server/resp/options"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var configFile string
var cmdIndexType, cmdDirPath, cmdPort string
var cmdSyncWrites *bool
var cmdMergeInterval, cmdDataFileSize *int64

var standaloneCmd = &cobra.Command{
	Use:   "standalone",
	Short: "Standalone kuloy",
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
			viper.Set("standalone.port", cmdPort)

			viper.Set("engine.writeSync", *cmdSyncWrites)
			viper.Set("engine.dirPath", cmdDirPath)
			viper.Set("engine.indexType", cmdIndexType)
			viper.Set("engine.mergeInterval", *cmdMergeInterval)
			viper.Set("engine.dataFileSize", *cmdDataFileSize)
		}

		addr := viper.GetString("standalone.addr")

		syncWrites := viper.GetBool("engine.syncWrites")
		dirPath := viper.GetString("engine.dirPath")
		dataFileSize := viper.GetInt64("engine.dataFileSize")
		indexType := viper.GetString("engine.indexType")
		mergeInterval := viper.GetInt64("engine.mergeInterval")

		var realAddr = addr
		host, realPort, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatal("Fail read Port: ", err)
			return
		}

		if host == "" {
			// If host is an empty string, it must be in the format of port
			addrs, err := net.InterfaceAddrs()
			if err != nil {
				fmt.Println("Failed to get local address: ", err)
				return
			}
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
					// Get an IPv4 address and concatenate it with the input port into a new string
					realAddr = ipnet.IP.String() + ":" + realPort
					return
				}
			}
			// If no IPv4 address is found, an error message is displayed
			log.Fatal("Error: Cannot get the local IPv4 address")
		}

		kuloyOpts := options.KuloyOptions{
			StandaloneOpt: CouloyDB.Options{
				DirPath:       dirPath,
				DataFileSize:  dataFileSize,
				SyncWrites:    syncWrites,
				MergeInterval: mergeInterval,
			},
		}
		switch indexType {
		case "hashmap":
			kuloyOpts.StandaloneOpt.IndexType = meta.Btree
		case "btree":
			kuloyOpts.StandaloneOpt.IndexType = meta.Btree
		case "art":
			kuloyOpts.StandaloneOpt.IndexType = meta.ART
		}

		resp.SetupEngine(kuloyOpts, false)
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

	srv := &server.TcpServer{
		Addr:             addr,
		Handler:          routerHandler,
		KeepAliveTimeout: 5 * time.Minute,
	}

	go func() {
		log.Printf("kuloy server running in %s \n", srv.Addr)
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
	standaloneCmd.Flags().StringVarP(&configFile, "cpath", "c", "", "Path of the configuration file in yaml, json and toml format (optional)")

	standaloneCmd.Flags().StringVarP(&cmdPort, "port", "p", ":9736", "Address of the host on the network (For example 192.168.1.151:9736) [default 0.0.0.0:9736]")

	standaloneCmd.Flags().StringVarP(&cmdDirPath, "dpath", "d", "./datafile", "Directory Path where data logs are stored [default at ./datafile]")
	standaloneCmd.Flags().StringVarP(&cmdIndexType, "itype", "t", "btree", "Type of memory index (hashmap/btree/art)")
	cmdMergeInterval = standaloneCmd.Flags().Int64P("minterval", "", 3600, "merge frequently interval (unit: second) [default 8 hours]")
	cmdDataFileSize = standaloneCmd.Flags().Int64P("dfsize", "", 268435456, "Maximum byte size per datafile (unit: Byte) [default 256MB]")
	cmdSyncWrites = standaloneCmd.Flags().BoolP("sync", "", false, "Whether to enable write synchronization (true/false)")

	root.AddCommand(standaloneCmd)
}
