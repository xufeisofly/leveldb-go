package leveldb

import (
	"bytes"

	"github.com/xufeisofly/leveldb-go/util"
)

type Comparator interface {
	Compare(a, b []byte) int8
	Name() string
	FindShortestSeparator(start *[]byte, limit []byte)
	FindShortSuccessor(key *[]byte)
}

type bytewiseComparator struct{}

var _ Comparator = (*bytewiseComparator)(nil)

func NewBytewiseComparator() *bytewiseComparator {
	return &bytewiseComparator{}
}

func (c *bytewiseComparator) Compare(a, b []byte) int8 {
	return int8(bytes.Compare(a, b))
}

func (c *bytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

// FindShortestSeparator shorten the start physically if *start < limit
// eg.
// *start: helloWorld
// limit: helloZookeeper
// because *start < limit, start -> helloX
func (c *bytewiseComparator) FindShortestSeparator(start *[]byte, limit []byte) {
	minLen := util.Min(len(*start), len(limit))
	// Find length of common prefix
	var diffIdx int
	for diffIdx < minLen && (*start)[diffIdx] == limit[diffIdx] {
		diffIdx++
	}

	if diffIdx >= minLen {
		// Do not shorten if one string is a prefix of the other
		return
	} else {
		diffByte := (*start)[diffIdx]
		if diffByte < 0xff && diffByte+1 < limit[diffIdx] {
			(*start)[diffIdx]++
			*start = (*start)[:diffIdx+1]
			if c.Compare(*start, limit) >= 0 {
				panic("start >= limit")
			}
		}
	}
}

// FindShortSuccessor change key to the shortest larger bytes
func (c *bytewiseComparator) FindShortSuccessor(key *[]byte) {
	for i := range *key {
		if (*key)[i] < 0xff {
			(*key)[i]++
			*key = (*key)[:i+1]
			return
		}
	}
}

type internalKeyComparator struct {
	comparator Comparator
}

var _ Comparator = (*internalKeyComparator)(nil)

func NewInternalKeyComparator(c Comparator) *internalKeyComparator {
	return &internalKeyComparator{c}
}

func (ic *internalKeyComparator) Compare(a, b []byte) int8 {
	return ic.comparator.Compare(a, b)
}

func (ic *internalKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

func (ic *internalKeyComparator) FindShortestSeparator(start *[]byte, limit []byte) {

}

func (ic *internalKeyComparator) FindShortSuccessor(key *[]byte) {
}
