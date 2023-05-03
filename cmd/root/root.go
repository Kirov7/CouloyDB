package root

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "kuloy",
	Short: "Less memory consumption, Larger storage capacity, and almost Constant read performance",
	Long:  `kuloy's goal is to compromise between performance and storage costs, as an alternative to Redis in some scenarios.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func Execute() {
	rootCmd.Execute()
}

func AddCommand(cmds ...*cobra.Command) {
	rootCmd.AddCommand(cmds...)
}
