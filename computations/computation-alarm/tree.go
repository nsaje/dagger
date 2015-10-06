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
	eval(valueTable valueTable) (bool, map[string][]float64)
	getLeafNodes() []LeafNode
}

type LeafNode struct {
	streamID           string
	relationalOperator RelationalOperator
	threshold          float64
	periods            int
}

func (n LeafNode) getLeafNodes() []LeafNode {
	return []LeafNode{n}
}

func (n LeafNode) eval(vt valueTable) (bool, map[string][]float64) {
	result := true
	values := make(map[string][]float64)
	values[n.streamID] = make([]float64, n.periods)
	lastNTuples := vt.getLastN(n.streamID, n.periods)
	if len(lastNTuples) == 0 {
		return false, values
	}
	for i, t := range lastNTuples {
		value := t.Data.(float64)
		values[n.streamID][i] = value
		switch n.relationalOperator {
		case LT:
			result = result && value < n.threshold
		case GT:
			result = result && value > n.threshold
		case LTE:
			result = result && value <= n.threshold
		case GTE:
			result = result && value >= n.threshold
		}
	}
	return result, values
}

type BinNode struct {
	op    LogicalOperator
	left  Node
	right Node
}

func mergeValues(l map[string][]float64, r map[string][]float64) map[string][]float64 {
	values := l
	for k, rightVal := range r {
		leftVal := l[k]
		// skip if key already exists and contains a longer slice (larger periods value)
		if leftVal != nil && len(leftVal) > len(rightVal) {
			continue
		}
		values[k] = rightVal
	}
	return values
}

func (n BinNode) eval(vt valueTable) (bool, map[string][]float64) {
	leftResult, leftValues := n.left.eval(vt)
	rightResult, rightValues := n.right.eval(vt)
	values := mergeValues(leftValues, rightValues)
	var result bool
	if n.op == OR {
		result = leftResult || rightResult
	} else if n.op == AND {
		result = leftResult && rightResult
	} else {
		panic("unknown operator")
	}
	return result, values
}

func (n BinNode) getLeafNodes() []LeafNode {
	return append(n.left.getLeafNodes(), n.right.getLeafNodes()...)
}
