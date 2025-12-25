package cmd

import (
	"fmt"
	"os"

	"github.com/sambasivareddy-ch/distributed-pg-stats/context"
	"github.com/spf13/cobra"
)

var metaQuery, database, user, password, host string
var port int16

/*
Entry point for this CLI.
Usage:

	./distributed-pg-stats \
		-d database_name \
		-H host \
		-P port \
		-u username \
		-p password \
		-q meta_query

	On execution, a config.json will be created and place local to the Dir
*/
var rootCmd = &cobra.Command{
	Use:   "Stats Collector",
	Short: "Stats collector for Distributed Postgres Systems",
	Run: func(cmd *cobra.Command, args []string) {
		var configCtx *context.Configuration = &context.Configuration{}

		configCtx.Database = database
		configCtx.User = user
		configCtx.Password = password
		configCtx.Host = host
		configCtx.MetaQuery = metaQuery
		configCtx.Port = port

		// Save the configuration details
		configCtx.SaveToFile()

		fmt.Println("Configuration loaded")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Initailizing the flags required to initialize
func init() {
	rootCmd.Flags().StringVarP(
		&database,
		"database",
		"d",
		"",
		"Database Name",
	)
	rootCmd.Flags().StringVarP(
		&host,
		"host",
		"H",
		"",
		"Database Host",
	)
	rootCmd.Flags().StringVarP(
		&user,
		"user",
		"u",
		"",
		"Database User",
	)
	rootCmd.Flags().StringVarP(
		&password,
		"password",
		"p",
		"",
		"Database Password",
	)
	rootCmd.Flags().StringVarP(
		&metaQuery,
		"metaquery",
		"q",
		"",
		"Database Meta Query",
	)
	rootCmd.Flags().Int16VarP(
		&port,
		"port",
		"P",
		5432,
		"Database Port",
	)

	rootCmd.MarkFlagRequired("database")
	rootCmd.MarkFlagRequired("host")
	rootCmd.MarkFlagRequired("user")
	rootCmd.MarkFlagRequired("password")
	rootCmd.MarkFlagRequired("metaquery")
	rootCmd.MarkFlagRequired("port")
}
