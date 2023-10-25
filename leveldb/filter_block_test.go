package leveldb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
	"github.com/xufeisofly/leveldb-go/util"
)

// for testing: emit an array with one hash value per key
type testHashFilterPolicy struct{}

var _ leveldb.FilterPolicy = (*testHashFilterPolicy)(nil)

func (f *testHashFilterPolicy) Name() string {
	return "TestHashFilterPolicy"
}

func (f *testHashFilterPolicy) CreateFilter(keys [][]byte, dst *[]byte) {
	for i := 0; i < len(keys); i++ {
		h := util.Hash(keys[i], 1)
		util.PutUint64Fixed(dst, uint64(h))
	}
}

func (f *testHashFilterPolicy) KeyMayMatch(key []byte, filter []byte) bool {
	h := util.Hash(key, 1)

	for i := 0; i+8 <= len(filter); i += 8 {
		if h == uint32(util.DecodeUint64Fixed(filter[i:])) {
			return true
		}
	}
	return false
}

func TestFilterBlock_EmptyBuilder(t *testing.T) {
	builder := leveldb.NewFilterBlockBuilder(&testHashFilterPolicy{})
	block := builder.Finish()

	assert.Equal(t, "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x0b", util.EscapeString(block))
}
