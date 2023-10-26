package leveldb_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
	"github.com/xufeisofly/leveldb-go/util"
)

const kVerbose int = 1

type bloomTest struct {
	policy leveldb.FilterPolicy
	filter []byte
	keys   [][]byte
}

func NewBloomTest() *bloomTest {
	return &bloomTest{
		policy: leveldb.NewBloomFilterPolicy(10),
	}
}

func (bt *bloomTest) Reset() {
	bt.keys = [][]byte{}
	bt.filter = []byte{}
}

func (bt *bloomTest) Add(s []byte) {
	bt.keys = append(bt.keys, s)
}

func (bt *bloomTest) Build() {
	bt.filter = []byte{}
	bt.policy.CreateFilter(bt.keys, &bt.filter)
	bt.keys = [][]byte{}

	if kVerbose >= 2 {
		bt.DumpFilter()
	}
}

func (bt *bloomTest) FilterSize() int {
	return len(bt.filter)
}

func (bt *bloomTest) DumpFilter() {
	fmt.Fprintf(os.Stderr, "F(")
	for i := 0; i < len(bt.filter)-1; i++ {
		c := uint(bt.filter[i])
		for j := 0; j < 8; j++ {
			s := '1'
			if c&(1<<j) == 0 {
				s = '.'
			}
			fmt.Fprintf(os.Stderr, "%c", s)
		}
	}
	fmt.Fprintf(os.Stderr, ")\n")
}

func (bt *bloomTest) Matches(s []byte) bool {
	if len(bt.keys) != 0 {
		bt.Build()
	}
	return bt.policy.KeyMayMatch(s, bt.filter)
}

func (bt *bloomTest) FalsePositiveRate() float64 {
	var result int
	for i := 0; i < 10000; i++ {
		if bt.Matches(key(i + 1000000000)) {
			result++
		}
	}
	return float64(result) / 10000.0
}

func key(i int) []byte {
	return util.EncodeUint64Fixed(uint64(i))
}

func TestBloom_EmptyFilter(t *testing.T) {
	b := NewBloomTest()
	assert.True(t, !b.Matches([]byte("hello")))
	assert.True(t, !b.Matches([]byte("world")))
}

func TestBloom_Small(t *testing.T) {
	b := NewBloomTest()
	b.Add([]byte("hello"))
	b.Add([]byte("world"))
	assert.True(t, b.Matches([]byte("hello")))
	assert.True(t, b.Matches([]byte("world")))
	assert.True(t, !b.Matches([]byte("x")))
	assert.True(t, !b.Matches([]byte("foo")))
}

func nextLength(length int) int {
	if length < 10 {
		length += 1
	} else if length < 100 {
		length += 10
	} else if length < 1000 {
		length += 100
	} else {
		length += 1000
	}
	return length
}

func TestBloom_VaryingLengths(t *testing.T) {
	b := NewBloomTest()
	var mediocreFilters int
	var goodFilters int

	for length := 1; length < 10000; length = nextLength(length) {
		b.Reset()
		for i := 0; i < length; i++ {
			b.Add(key(i))
		}
		b.Build()

		// The c++ code is length + 10/8 + 40 here, have no idea what 40 stands for.
		assert.Less(t, b.FilterSize(), length*10/8+1+8, length)

		// All added keys must match
		for i := 0; i < length; i++ {
			assert.True(t, b.Matches(key(i)), fmt.Sprintf("Length %d; key %d", length, i))
		}

		// Check false positive rate
		rate := b.FalsePositiveRate()
		if kVerbose >= 1 {
			fmt.Fprintf(os.Stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n", rate*100.0, length, b.FilterSize())
		}
		assert.Less(t, rate, 0.03) // Must not be over 3%

		if rate > 0.0125 {
			mediocreFilters++
		} else {
			goodFilters++
		}
	}

	if kVerbose >= 1 {
		fmt.Fprintf(os.Stderr, "Filters: %d good, %d mediocre\n", goodFilters, mediocreFilters)
	}
	assert.Less(t, mediocreFilters, goodFilters/5)
}
