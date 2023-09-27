package leveldb_test

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
	"github.com/xufeisofly/leveldb-go/util"
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

	iter := leveldb.NewSkiplistIterator(list)
	assert.True(t, !iter.Valid())
	iter.SeekToFirst()
	assert.True(t, !iter.Valid())
	iter.Seek([]byte("100"))
	assert.True(t, !iter.Valid())
	iter.SeekToLast()
	assert.True(t, !iter.Valid())
}

func TestSkiplist_InsertAndLookup(t *testing.T) {
	N := 2000
	R := 5000

	keySet := make(map[int]struct{}, 0)
	keyArr := make([]int, 0)
	cmp := NewTestComparator()
	list := leveldb.NewSkiplist(cmp)

	begin := 0

	for i := begin; i < N; i++ {
		rand.Seed(time.Now().UnixNano())
		keyInt := rand.Intn(1000) % R
		if _, ok := keySet[keyInt]; !ok {
			keySet[keyInt] = struct{}{}
			keyArr = append(keyArr, keyInt)
			list.Insert(util.EncodeUvarint(uint64(keyInt)))
		}
	}

	// sort keyArr by cmp
	sort.Slice(keyArr, func(i, j int) bool {
		return cmp.Compare(util.EncodeUvarint(uint64(keyArr[i])),
			util.EncodeUvarint(uint64(keyArr[j]))) < 0
	})

	for i := begin; i < R; i++ {
		key := util.EncodeUvarint(uint64(i))
		_, ok := keySet[i]
		if list.Contains(key) {
			assert.True(t, ok)
		} else {
			assert.False(t, ok)
		}
	}

	// test iterator
	iter := leveldb.NewSkiplistIterator(list)
	assert.True(t, !iter.Valid())

	iter.Seek(util.EncodeUvarint(0))
	assert.True(t, iter.Valid())
	assert.Equal(t, util.EncodeUvarint(0), iter.Key())

	iter.SeekToFirst()
	assert.True(t, iter.Valid())
	assert.Equal(t, util.EncodeUvarint(uint64(keyArr[0])), iter.Key())

	iter.SeekToLast()
	assert.True(t, iter.Valid())
	assert.Equal(t, util.EncodeUvarint(uint64(keyArr[len(keyArr)-1])), iter.Key())

	// Forward iteration test
	iter = leveldb.NewSkiplistIterator(list)
	iter.SeekToFirst()

	for i := 0; i < len(keyArr); i++ {
		assert.Equal(t, iter.Key(), util.EncodeUvarint(uint64(keyArr[i])))
		iter.Next()
	}

	// Backword iteration test
	iter = leveldb.NewSkiplistIterator(list)
	iter.SeekToLast()

	for i := len(keyArr) - 1; i >= 0; i-- {
		assert.Equal(t, iter.Key(), util.EncodeUvarint(uint64(keyArr[i])))
		iter.Prev()
	}
}
