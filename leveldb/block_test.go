package leveldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var entrymap = map[string]string{
	"aaaaa": "1",
	"aabbb": "2",
	"abbbb": "3",
	"bcccc": "4",
	"ccccc": "5",
	"ccddd": "6",
}

var testKeys = []string{
	"aaaaa",
	"aabbb",
	"abbbb",
	"bcccc",
	"ccccc",
	"ccddd",
}

func prepareBlockBuilder() *blockBuilder {
	options := DefaultOptions
	options.BlockRestartInternal = 3

	bb := NewBlockBuilder(options)

	for _, testKey := range testKeys {
		bb.Add([]byte(testKey), []byte(entrymap[testKey]))
	}

	bb.Finish()
	return bb
}

func TestBlock_NumRestarts(t *testing.T) {
	bb := prepareBlockBuilder()
	block := NewBlock(&blockContents{
		data: bb.buffer,
	})

	assert.Equal(t, uint64(len(bb.restarts)), block.NumRestarts())
}

func TestBlockIterator_Empty(t *testing.T) {
	block := NewBlock(&blockContents{
		data: []byte{},
	})
	bIter := block.NewIterator(NewBytewiseComparator())

	assert.True(t, !bIter.Valid())
}

func TestBlockIterator_Seek(t *testing.T) {
	bb := prepareBlockBuilder()
	block := NewBlock(&blockContents{
		data: bb.buffer,
	})

	bIter := block.NewIterator(NewBytewiseComparator())

	testcases := []struct {
		actualKey string
		expectKey string
		valid     bool
	}{
		{"", "aaaaa", true},
		{"bcccc", "bcccc", true},
		{"abbbb", "abbbb", true},
		{"beeee", "ccccc", true},
		{"zzzzz", "aaaaa", false},
	}

	for _, testcase := range testcases {
		bIter.Seek([]byte(testcase.actualKey))
		assert.Equal(t, testcase.valid, bIter.Valid())
		if bIter.Valid() {
			assert.Equal(t, testcase.expectKey, string(bIter.Key()))
			assert.Equal(t, entrymap[string(bIter.Key())], string(bIter.Value()))
		}
	}

	bIter.SeekToFirst()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[0], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[0]], string(bIter.Value()))

	bIter.Next()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[1], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[1]], string(bIter.Value()))

	bIter.Next()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[2], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[2]], string(bIter.Value()))

	bIter.Next()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[3], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[3]], string(bIter.Value()))

	bIter.Prev()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[2], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[2]], string(bIter.Value()))

	bIter.Prev()
	assert.True(t, bIter.Valid())
	assert.Equal(t, testKeys[1], string(bIter.Key()))
	assert.Equal(t, entrymap[testKeys[1]], string(bIter.Value()))

	bIter.SeekToLast()
	assert.True(t, bIter.Valid())
	assert.Equal(t, entrymap[testKeys[len(testKeys)-1]], string(bIter.Value()))
}
