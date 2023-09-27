package leveldb

import "bytes"

type Comparator interface {
	Compare(a, b []byte) int8
}

type bytewiseComparator struct{}

var _ Comparator = (*bytewiseComparator)(nil)

func NewBytewiseComparator() *bytewiseComparator {
	return &bytewiseComparator{}
}

func (c *bytewiseComparator) Compare(a, b []byte) int8 {
	return int8(bytes.Compare(a, b))
}
