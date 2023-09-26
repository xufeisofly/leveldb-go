package leveldb_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
)

type testComparator struct {
}

var _ leveldb.Comparator = (*testComparator)(nil)

func (c *testComparator) Compare(a, b []byte) int8 {
	return int8(bytes.Compare(a, b))
}

func NewTestComparator() leveldb.Comparator {
	return &testComparator{}
}

func TestSkiplist_Empty(t *testing.T) {
	testCmp := NewTestComparator()
	list := leveldb.NewSkiplist(testCmp)
	assert.True(t, !list.Contains([]byte("10")))
}
