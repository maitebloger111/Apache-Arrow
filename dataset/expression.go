// Package dataset provides an optimized dataset scan engine with
// early partition pruning, parallel fragment filtering, and
// expression simplification caching.
package dataset

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// ExpressionOp represents the type of expression operation.
type ExpressionOp int

const (
	OpLiteral ExpressionOp = iota
	OpField
	OpEqual
	OpNotEqual
	OpGreater
	OpGreaterEqual
	OpLess
	OpLessEqual
	OpAnd
	OpOr
	OpNot
	OpIn
)

// Expression represents a filter expression tree.
type Expression struct {
	Op     ExpressionOp
	Field  string
	Value  interface{}
	Values []interface{} // for IN clauses
	Left   *Expression
	Right  *Expression
}

// String returns a human-readable representation of the expression.
func (e *Expression) String() string {
	if e == nil {
		return "true"
	}
	switch e.Op {
	case OpLiteral:
		return fmt.Sprintf("%v", e.Value)
	case OpField:
		return e.Field
	case OpEqual:
		return fmt.Sprintf("%s == %v", e.Field, e.Value)
	case OpNotEqual:
		return fmt.Sprintf("%s != %v", e.Field, e.Value)
	case OpGreater:
		return fmt.Sprintf("%s > %v", e.Field, e.Value)
	case OpGreaterEqual:
		return fmt.Sprintf("%s >= %v", e.Field, e.Value)
	case OpLess:
		return fmt.Sprintf("%s < %v", e.Field, e.Value)
	case OpLessEqual:
		return fmt.Sprintf("%s <= %v", e.Field, e.Value)
	case OpAnd:
		return fmt.Sprintf("(%s && %s)", e.Left, e.Right)
	case OpOr:
		return fmt.Sprintf("(%s || %s)", e.Left, e.Right)
	case OpNot:
		return fmt.Sprintf("!(%s)", e.Left)
	case OpIn:
		return fmt.Sprintf("%s IN %v", e.Field, e.Values)
	}
	return "unknown"
}

// PartitionKey represents a single partition key-value pair.
type PartitionKey struct {
	Field string
	Value interface{}
	// IsDictionary indicates the partition value is dictionary-encoded.
	IsDictionary bool
	// Decoded caches the decoded value for dictionary-encoded keys.
	// Set to nil until first decode.
	Decoded interface{}
}

// Decode returns the decoded value for dictionary-encoded keys
// without redundant decoding if already decoded.
func (k *PartitionKey) Decode() interface{} {
	if k.IsDictionary && k.Decoded == nil {
		// Simulate dictionary decode cost once.
		k.Decoded = k.Value
	}
	if !k.IsDictionary {
		return k.Value
	}
	return k.Decoded
}

// PartitionExpr represents a fragment's partition expression,
// i.e., the set of partition keys that define this fragment.
type PartitionExpr struct {
	Keys []PartitionKey
}

// Matches checks whether this partition expression is compatible
// with the given filter expression. This is the core partition
// pruning function.
func (pe *PartitionExpr) Matches(filter *Expression) bool {
	if filter == nil {
		return true
	}
	return pe.evaluate(filter)
}

func (pe *PartitionExpr) evaluate(e *Expression) bool {
	if e == nil {
		return true
	}
	switch e.Op {
	case OpLiteral:
		b, ok := e.Value.(bool)
		return !ok || b
	case OpField:
		// A bare field reference is truthy if non-zero.
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				switch val := v.(type) {
				case string:
					return val != ""
				case int:
					return val != 0
				}
				return v != nil
			}
		}
		return true // unknown field passes through
	case OpEqual:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return compareEqual(v, e.Value)
			}
		}
		return true // field not in partition — can't prune
	case OpNotEqual:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return !compareEqual(v, e.Value)
			}
		}
		return true
	case OpGreater:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return compareGreater(v, e.Value)
			}
		}
		return true
	case OpGreaterEqual:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return compareGreater(v, e.Value) || compareEqual(v, e.Value)
			}
		}
		return true
	case OpLess:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return !compareGreater(v, e.Value) && !compareEqual(v, e.Value)
			}
		}
		return true
	case OpLessEqual:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				return !compareGreater(v, e.Value)
			}
		}
		return true
	case OpAnd:
		return pe.evaluate(e.Left) && pe.evaluate(e.Right)
	case OpOr:
		return pe.evaluate(e.Left) || pe.evaluate(e.Right)
	case OpNot:
		return !pe.evaluate(e.Left)
	case OpIn:
		for _, k := range pe.Keys {
			if k.Field == e.Field {
				v := k.Decode()
				for _, val := range e.Values {
					if compareEqual(v, val) {
						return true
					}
				}
				return false
			}
		}
		return true
	}
	return true
}

func compareEqual(a, b interface{}) bool {
	// Type-coercive comparison to match Arrow's semantics.
	switch va := a.(type) {
	case string:
		vb, ok := b.(string)
		return ok && va == vb
	case int:
		switch vb := b.(type) {
		case int:
			return va == vb
		case int64:
			return int64(va) == vb
		case float64:
			return float64(va) == vb
		}
	case int64:
		switch vb := b.(type) {
		case int:
			return va == int64(vb)
		case int64:
			return va == vb
		case float64:
			return float64(va) == vb
		}
	case float64:
		switch vb := b.(type) {
		case int:
			return va == float64(vb)
		case int64:
			return va == float64(vb)
		case float64:
			return va == vb
		}
	}
	return false
}

func compareGreater(a, b interface{}) bool {
	switch va := a.(type) {
	case int:
		switch vb := b.(type) {
		case int:
			return va > vb
		case int64:
			return int64(va) > vb
		case float64:
			return float64(va) > vb
		}
	case int64:
		switch vb := b.(type) {
		case int:
			return va > int64(vb)
		case int64:
			return va > vb
		case float64:
			return float64(va) > vb
		}
	case float64:
		switch vb := b.(type) {
		case int:
			return va > float64(vb)
		case int64:
			return va > float64(vb)
		case float64:
			return va > vb
		}
	case string:
		vb, ok := b.(string)
		return ok && va > vb
	}
	return false
}

// expressionKey is used as a cache key for simplified expressions.
type expressionKey struct {
	filterHash  uint64
	partitionID string
}

// ExpressionSimplifier provides cached expression simplification.
// Given a filter expression and a partition guarantee, it produces
// a simplified expression. Results are cached so identical
// (filter, partition) pairs reuse previously computed results,
// avoiding redundant AST traversals.
type ExpressionSimplifier struct {
	mu    sync.RWMutex
	cache map[expressionKey]*Expression
	hits  int64
	miss  int64
}

// NewExpressionSimplifier creates a new simplifier with an empty cache.
func NewExpressionSimplifier() *ExpressionSimplifier {
	return &ExpressionSimplifier{
		cache: make(map[expressionKey]*Expression),
	}
}

// CacheHits returns the number of cache hits.
func (s *ExpressionSimplifier) CacheHits() int64 {
	return atomic.LoadInt64(&s.hits)
}

// CacheMisses returns the number of cache misses.
func (s *ExpressionSimplifier) CacheMisses() int64 {
	return atomic.LoadInt64(&s.miss)
}

// SimplifyWithGuarantee simplifies the filter expression given a
// partition guarantee. For example, if the filter is
// "a == 1 AND b == 2" and the partition guarantee is "a == 1",
// the simplified expression becomes "b == 2".
func (s *ExpressionSimplifier) SimplifyWithGuarantee(filter *Expression, guarantee *PartitionExpr) *Expression {
	if filter == nil || guarantee == nil {
		return filter
	}

	// Build cache key.
	key := expressionKey{
		filterHash:  hashExpression(filter),
		partitionID: guarantee.String(),
	}

	// Check cache.
	s.mu.RLock()
	cached, ok := s.cache[key]
	s.mu.RUnlock()
	if ok {
		atomic.AddInt64(&s.hits, 1)
		return cached
	}
	atomic.AddInt64(&s.miss, 1)

	// Compute simplified expression.
	simplified := s.simplify(filter, guarantee)

	// Store in cache.
	s.mu.Lock()
	s.cache[key] = simplified
	s.mu.Unlock()

	return simplified
}

func (s *ExpressionSimplifier) simplify(e *Expression, guarantee *PartitionExpr) *Expression {
	if e == nil {
		return nil
	}

	switch e.Op {
	case OpAnd:
		left := s.simplify(e.Left, guarantee)
		right := s.simplify(e.Right, guarantee)
		// false AND X → false; X AND false → false
		if isLiteralFalse(left) || isLiteralFalse(right) {
			return &Expression{Op: OpLiteral, Value: false}
		}
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		return &Expression{Op: OpAnd, Left: left, Right: right}

	case OpOr:
		left := s.simplify(e.Left, guarantee)
		right := s.simplify(e.Right, guarantee)
		if left == nil && right == nil {
			return nil
		}
		if left == nil {
			return right
		}
		if right == nil {
			return left
		}
		return &Expression{Op: OpOr, Left: left, Right: right}

	case OpNot:
		inner := s.simplify(e.Left, guarantee)
		if inner == nil {
			return nil
		}
		return &Expression{Op: OpNot, Left: inner}

	case OpEqual, OpNotEqual, OpGreater, OpGreaterEqual, OpLess, OpLessEqual, OpIn:
		// Check if this comparison is guaranteed by the partition.
		for _, k := range guarantee.Keys {
			if k.Field == e.Field {
				if e.Op == OpEqual {
					if compareEqual(k.Decode(), e.Value) {
						return nil // guaranteed true, can remove
					}
					return &Expression{Op: OpLiteral, Value: false}
				}
			}
		}
		return e

	default:
		return e
	}
}

// String returns a hashable string representation of partition keys.
func (pe *PartitionExpr) String() string {
	var parts []string
	for _, k := range pe.Keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k.Field, k.Value))
	}
	return strings.Join(parts, ",")
}

// hashExpression computes a simple hash of an expression tree.
func hashExpression(e *Expression) uint64 {
	if e == nil {
		return 0
	}
	h := uint64(e.Op)
	if e.Field != "" {
		for _, c := range e.Field {
			h = h*31 + uint64(c)
		}
	}
	if e.Left != nil {
		h = h*7 + hashExpression(e.Left)
	}
	if e.Right != nil {
		h = h*7 + hashExpression(e.Right)
	}
	return h
}

// isLiteralFalse returns true if the expression is the literal false.
func isLiteralFalse(e *Expression) bool {
	if e == nil {
		return false
	}
	if e.Op == OpLiteral {
		if b, ok := e.Value.(bool); ok && !b {
			return true
		}
	}
	return false
}
