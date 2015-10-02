package main

type RelationalOperator int

const (
	LT RelationalOperator = iota
	GT
	LTE
	GTE
)

type LogicalOperator int

const (
	OR LogicalOperator = iota
	AND
)

type Node interface {
	eval(values map[string]float64) bool
	getStreamIDs() []string
}

type LeafNode struct {
	streamID           string
	relationalOperator RelationalOperator
	threshold          float64
	times              int
}

func (n LeafNode) getStreamIDs() []string {
	return []string{n.streamID}
}

func (n LeafNode) eval(values map[string]float64) bool {
	switch n.relationalOperator {
	case LT:
		return values[n.streamID] < n.threshold
	case GT:
		return values[n.streamID] > n.threshold
	case LTE:
		return values[n.streamID] <= n.threshold
	case GTE:
		return values[n.streamID] >= n.threshold
	}
	return false
}

type BinNode struct {
	op    LogicalOperator
	left  Node
	right Node
}

func (n BinNode) eval(values map[string]float64) bool {
	if n.op == OR {
		return n.left.eval(values) || n.right.eval(values)
	} else if n.op == AND {
		return n.left.eval(values) && n.right.eval(values)
	} else {
		panic("unknown operator")
	}
}

func (n BinNode) getStreamIDs() []string {
	return append(n.left.getStreamIDs(), n.right.getStreamIDs()...)
}
