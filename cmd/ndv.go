package cmd

import (
	"log"

	"github.com/sambasivareddy-ch/distributed-pg-stats/context"
	"github.com/sambasivareddy-ch/distributed-pg-stats/ndvs"

	"github.com/spf13/cobra"
)

/*
Command to load the NDV (Number of Distinct Values)

	Here, this command loads the config again and
	connect to the coordinator and loads the NDVs

Usage:

	./distributed-pg-stats load-ndv
*/
var ndvCmd = &cobra.Command{
	Use:   "load-ndv",
	Short: "Command to load the Global NDVs",
	Run: func(cmd *cobra.Command, args []string) {
		context.LoadSession()
		config := context.GlobalConfigCtx

		// Connect to Coordinator using the configured details
		connection, err := ndvs.ConnectToPostgres(
			config.Host,
			config.User,
			config.Database,
			config.Port)

		if err != nil {
			log.Panic(err)
		}

		// Load the NDVs
		ndvs.LoadNDVs(connection, true, config.MetaQuery)
	},
}

func init() {
	rootCmd.AddCommand(ndvCmd)
}
