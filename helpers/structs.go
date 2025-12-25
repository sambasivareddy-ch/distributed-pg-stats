package helpers

// Join edge with cost
type JoinEdge struct {
	LeftTable   string
	LeftColumn  string
	RightTable  string
	RightColumn string
	Cost        float64
}
