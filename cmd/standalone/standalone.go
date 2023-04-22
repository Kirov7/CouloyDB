package standalone

import (
	"fmt"
	"github.com/Kirov7/CouloyDB/cmd/root"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
)

var configFile string
var cmdIndexType, cmdDirPath string
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
			viper.Set("engine.writeSync", *cmdSyncWrites)
			viper.Set("engine.indexType", cmdIndexType)
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
	standaloneCmd.Flags().StringVarP(&configFile, "cpath", "f", "", "Path of the configuration file in yaml, json and toml format (optional)")
	standaloneCmd.Flags().StringVarP(&cmdDirPath, "dpath", "d", "./datafile", "Directory Path where data logs are stored [default at ./datafile]")
	standaloneCmd.Flags().StringVarP(&cmdIndexType, "itype", "t", "btree", "Type of memory index (hashmap/btree/art)")
	cmdMergeInterval = standaloneCmd.Flags().Int64P("minterval", "", 3600, "merge frequently interval (unit: second) [default 8 hours]")
	cmdDataFileSize = standaloneCmd.Flags().Int64P("dfsize", "", 268435456, "Maximum byte size per datafile (unit: Byte) [default 256MB]")
	cmdSyncWrites = standaloneCmd.Flags().BoolP("sync", "", false, "Whether to enable write synchronization (true/false)")

	root.AddCommand(standaloneCmd)
}
