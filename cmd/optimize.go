package cmd

import (
	"fmt"
	"strings"

	"github.com/sambasivareddy-ch/distributed-pg-stats/context"
	"github.com/sambasivareddy-ch/distributed-pg-stats/helpers"
	"github.com/sambasivareddy-ch/distributed-pg-stats/optimizer"

	"github.com/spf13/cobra"
)

var tables []string
var joins []string

/*
This command takes the tables and joins condition as the output
it gives the optimized join order.

Usage:

	./distributed-pg-stats optimize \
		--table table1 \
		--table table2 \
		....
		--join table1.col1=table2.col1 \
		--join table1.col2=table2.col2 \
		...
*/
var optimizeCmd = &cobra.Command{
	Use:   "optimize",
	Short: "Generate best join order using global NDV stats",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Gives the optimized order
		return runOptimize()
	},
}

// Flags for 'optimize' command
func init() {
	rootCmd.AddCommand(optimizeCmd)

	optimizeCmd.Flags().StringSliceVar(
		&tables,
		"table",
		nil,
		"Tables involved in the query",
	)

	optimizeCmd.Flags().StringSliceVar(
		&joins,
		"join",
		nil,
		"Join predicates: table.col=table.col",
	)

	optimizeCmd.MarkFlagRequired("table")
	optimizeCmd.MarkFlagRequired("join")
}

func runOptimize() error {
	// Use the joins and build the join edges
	edges, err := optimizer.BuildJoinEdges(joins)
	if err != nil {
		return err
	}

	context.LoadSession()
	config := context.GlobalConfigCtx
	connection, _ := helpers.ConnectToPostgres(config.Host, config.User, config.Database, config.Port)

	ndvStats, err := helpers.LoadNDVs(connection, false, config.MetaQuery)
	if err != nil {
		return err
	}

	// Use the edges to estimate the cost of each join using NDVs
	if err := optimizer.AssignJoinCosts(edges, ndvStats); err != nil {
		return err
	}

	// Now compute the join order based on cost-assigned edges
	joinOrder := optimizer.ComputeJoinOrder(tables, edges)

	fmt.Println("Best Join Order:")
	for i, t := range joinOrder {
		fmt.Printf("%d. %s\n", i+1, t)
	}

	if err := helpers.InsertOptimizedJoinOrderIntoMeta(connection, 1, strings.Join(joinOrder, ",")); err != nil {
		fmt.Println(err)
	}

	return nil
}
