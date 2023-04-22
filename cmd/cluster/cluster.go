package cluster

import (
	"fmt"
	"github.com/Kirov7/CouloyDB/cmd/root"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strings"
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

		writeSync := viper.GetBool("engine.writeSync")
		indexType := viper.GetString("engine.indexType")
		mergeInterval := viper.GetInt("engine.mergeInterval")

		// Output parsed parameter values
		fmt.Printf("addr List: %v\n", cluster)
		fmt.Printf("Location of the machine: %d\n", self)
		fmt.Printf("Whether to enable write synchronization: %t\n", writeSync)
		fmt.Printf("Use index types: %s\n", indexType)
		fmt.Printf("merge interval is frequent: %d秒\n", mergeInterval)
	},
}

func init() {
	clusterCmd.Flags().StringVarP(&configFile, "cpath", "f", "", "Path of the configuration file in yaml, json and toml format (optional)")

	clusterCmd.Flags().StringVarP(&cmdPeers, "peers", "c", "", "All the instances in the cluster ip:port, separated by commas (such as 192.168.1.1:8080,192168:1.2:8080)")
	cmdSelf = clusterCmd.Flags().Int64P("self", "s", 0, "Local Index in the cluster (optional)")

	clusterCmd.Flags().StringVarP(&cmdDirPath, "dpath", "d", "./datafile", "Directory Path where data logs are stored [default at ./datafile]")
	clusterCmd.Flags().StringVarP(&cmdIndexType, "itype", "t", "btree", "Type of memory index (hashmap/btree/art)")
	cmdMergeInterval = clusterCmd.Flags().Int64P("minterval", "", 3600, "merge frequently interval (unit: second) [default 8 hours]")
	cmdDataFileSize = clusterCmd.Flags().Int64P("dfsize", "", 268435456, "Maximum byte size per datafile (unit: Byte) [default 256MB]")
	cmdSyncWrites = clusterCmd.Flags().BoolP("sync", "", false, "Whether to enable write synchronization (true/false)")

	root.AddCommand(clusterCmd)
}
