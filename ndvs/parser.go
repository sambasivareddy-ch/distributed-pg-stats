package ndvs

import (
	"fmt"
	"strings"

	"github.com/sambasivareddy-ch/distributed-pg-stats/helpers"
)

// Parse the join condition given and generate a edge with cost == 0
func ParseJoinPredicate(expr string) (helpers.JoinEdge, error) {
	parts := strings.Split(expr, "=")
	if len(parts) != 2 {
		return helpers.JoinEdge{}, fmt.Errorf("invalid join predicate: %s", expr)
	}

	left := strings.Split(parts[0], ".")
	right := strings.Split(parts[1], ".")

	if len(left) != 2 || len(right) != 2 {
		return helpers.JoinEdge{}, fmt.Errorf("invalid join predicate: %s", expr)
	}

	return helpers.JoinEdge{
		LeftTable:   left[0],
		LeftColumn:  left[1],
		RightTable:  right[0],
		RightColumn: right[1],
	}, nil
}

// Iterates through each of join condition given and generates the join edges
func BuildJoinEdges(joinExprs []string) ([]helpers.JoinEdge, error) {
	var edges []helpers.JoinEdge

	for _, expr := range joinExprs {
		edge, err := ParseJoinPredicate(expr)
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}

	return edges, nil
}
