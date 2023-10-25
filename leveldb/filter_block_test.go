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
	policy := &testHashFilterPolicy{}
	builder := leveldb.NewFilterBlockBuilder(policy)
	block := builder.Finish()

	assert.Equal(t, "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x0b", util.EscapeString(block))

	reader := leveldb.NewFilterBlockReader(policy, block)
	assert.True(t, reader.KeyMayMatch(0, []byte("foo")))
	assert.True(t, reader.KeyMayMatch(100000, []byte("foo")))
}

func TestFilterBlock_SingleChunk(t *testing.T) {
	policy := &testHashFilterPolicy{}
	builder := leveldb.NewFilterBlockBuilder(policy)
	builder.StartBlock(100)
	builder.AddKey([]byte("foo"))
	builder.AddKey([]byte("bar"))
	builder.AddKey([]byte("box"))
	builder.StartBlock(200)
	builder.AddKey([]byte("box"))
	builder.StartBlock(300)
	builder.AddKey([]byte("hello"))
	block := builder.Finish()
	reader := leveldb.NewFilterBlockReader(policy, block)
	assert.True(t, reader.KeyMayMatch(100, []byte("foo")))
	assert.True(t, reader.KeyMayMatch(100, []byte("bar")))
	assert.True(t, reader.KeyMayMatch(100, []byte("box")))
	assert.True(t, reader.KeyMayMatch(100, []byte("hello")))
	assert.True(t, reader.KeyMayMatch(100, []byte("foo")))
	assert.True(t, !reader.KeyMayMatch(100, []byte("missing")))
	assert.True(t, !reader.KeyMayMatch(100, []byte("other")))
}
