package ndvs

import (
	"fmt"
	"math"
	"sort"

	"github.com/sambasivareddy-ch/distributed-pg-stats/helpers"
)

// Function to assign the cost of a edge
func AssignJoinCosts(edges []helpers.JoinEdge, ndv map[string]map[string]float64) error {
	for i := range edges {
		// Get the NDV of the columns involved in join condition
		l, ok1 := ndv[edges[i].LeftTable][edges[i].LeftColumn]
		r, ok2 := ndv[edges[i].RightTable][edges[i].RightColumn]

		if !ok1 || !ok2 {
			return fmt.Errorf(
				"NDV not found for %s.%s or %s.%s",
				edges[i].LeftTable, edges[i].LeftColumn,
				edges[i].RightTable, edges[i].RightColumn,
			)
		}

		/*
			Why MAX?
			In INNER JOIN (join) case, when two columns joined, the number of rows returned
			will be equals to the column's rows which has large number of rows.
		*/
		edges[i].Cost = math.Max(l, r)
	}
	return nil
}

// Computes the join order based on Cost (using Greedy algorithm)
func ComputeJoinOrder(tables []string, edges []helpers.JoinEdge) []string {
	joined := make(map[string]bool)
	var order []string

	// Sort the edges based on their cost
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Cost < edges[j].Cost
	})

	first := edges[0]
	order = append(order, first.LeftTable, first.RightTable)
	joined[first.LeftTable] = true
	joined[first.RightTable] = true

	for len(joined) < len(tables) {
		bestCost := math.MaxFloat64
		var next string

		// Get the next possible best edge using costs
		for _, e := range edges {
			if joined[e.LeftTable] && !joined[e.RightTable] && e.Cost < bestCost {
				bestCost = e.Cost
				next = e.RightTable
			}
			if joined[e.RightTable] && !joined[e.LeftTable] && e.Cost < bestCost {
				bestCost = e.Cost
				next = e.LeftTable
			}
		}

		if next == "" {
			break
		}

		joined[next] = true
		order = append(order, next)
	}

	return order
}
