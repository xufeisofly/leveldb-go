package leveldb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockBuilder_Add(t *testing.T) {
	options := DefaultOptions
	options.BlockRestartInternal = 3

	bb := NewBlockBuilder(options)

	testcases := []struct {
		key   string
		value string
	}{
		{"aaaaa", "1"},
		{"aabbb", "2"},
		{"abbbb", "3"},
		{"bcccc", "4"},
		{"ccccc", "5"},
		{"ccddd", "6"},
	}

	for _, testcase := range testcases {
		err := bb.Add([]byte(testcase.key), []byte(testcase.value))
		assert.NoError(t, err)
	}

	assert.Equal(t, false, bb.finished)
	bb.Finish()
	assert.Equal(t, true, bb.finished)
	assert.Equal(t, (len(testcases)-1)/options.BlockRestartInternal+1, len(bb.restarts))
	assert.Equal(t, (len(testcases)-1)%options.BlockRestartInternal+1, bb.counter)
	assert.Equal(t, testcases[len(testcases)-1].key, string(bb.lastKey))
}
