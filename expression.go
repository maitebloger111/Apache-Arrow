package main

import "fmt"

// Op represents a comparison operator for filter expressions.
type Op int

const (
	Eq Op = iota
	NotEq
	Lt
	LtEq
	Gt
	GtEq
)

// Expression represents a filter expression that can be evaluated
// against partition key-value pairs.
type Expression struct {
	Field string
	Op    Op
	Value interface{}
}

// String returns a human-readable representation of the expression.
func (e Expression) String() string {
	ops := map[Op]string{Eq: "==", NotEq: "!=", Lt: "<", LtEq: "<=", Gt: ">", GtEq: ">="}
	return fmt.Sprintf("%s %s %v", e.Field, ops[e.Op], e.Value)
}

// Evaluate checks whether the expression matches the given partition
// key-value pairs. Returns true if the partition satisfies the filter.
func (e Expression) Evaluate(partitionValues map[string]interface{}) bool {
	val, ok := partitionValues[e.Field]
	if !ok {
		// Field not in partition keys — can't prune, assume match.
		return true
	}
	return compare(val, e.Op, e.Value)
}

// compare performs a typed comparison between two values.
func compare(left interface{}, op Op, right interface{}) bool {
	switch l := left.(type) {
	case int:
		r, ok := right.(int)
		if !ok {
			return false
		}
		return compareInt(l, op, r)
	case string:
		r, ok := right.(string)
		if !ok {
			return false
		}
		return compareString(l, op, r)
	default:
		return false
	}
}

func compareInt(l int, op Op, r int) bool {
	switch op {
	case Eq:
		return l == r
	case NotEq:
		return l != r
	case Lt:
		return l < r
	case LtEq:
		return l <= r
	case Gt:
		return l > r
	case GtEq:
		return l >= r
	default:
		return false
	}
}

func compareString(l string, op Op, r string) bool {
	switch op {
	case Eq:
		return l == r
	case NotEq:
		return l != r
	case Lt:
		return l < r
	case LtEq:
		return l <= r
	case Gt:
		return l > r
	case GtEq:
		return l >= r
	default:
		return false
	}
}
