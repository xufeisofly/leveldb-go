package leveldb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
	"github.com/xufeisofly/leveldb-go/util"
)

func TestLookupKey(t *testing.T) {
	userKey := util.EncodeUvarint(uint64(127))
	seq := leveldb.SequenceNumber(1)
	lkey := leveldb.NewLookupKey(userKey, seq)

	assert.Equal(t, userKey, lkey.UserKey())
	assert.Equal(t, userKey, lkey.InternalKey()[:len(lkey.InternalKey())-leveldb.TagSize])
}

func testKey(t *testing.T, ukey []byte, seq leveldb.SequenceNumber, valueType leveldb.ValueType) {
	parsedIKey := leveldb.NewParsedInternalKey(ukey, seq, valueType)

	ikeyBytes := leveldb.DumpInternalKey(parsedIKey)
	ret, err := leveldb.ParseInternalKey(ikeyBytes)
	assert.NoError(t, err)
	assert.Equal(t, parsedIKey.UserKey, ret.UserKey)
	assert.Equal(t, parsedIKey.Sequence, ret.Sequence)
	assert.Equal(t, parsedIKey.Type, ret.Type)
}

func TestInternalKey_DumpAndParse(t *testing.T) {
	testKeys := []string{"", "k", "hello", "longggggggggggggggggggggg"}
	testSeqs := []leveldb.SequenceNumber{
		1,
		2,
		3,
		(1 << 8) - 1,
		(1 << 8),
		(1 << 8) + 1,
		(1 << 16) - 1,
		(1 << 16),
		(1 << 16) + 1,
		(1 << 32) - 1,
		(1 << 32),
		(1 << 32) + 1,
	}

	for _, ukey := range testKeys {
		for _, seq := range testSeqs {
			testKey(t, []byte(ukey), seq, leveldb.ValueType_Value)
			testKey(t, []byte(ukey), seq, leveldb.ValueType_Deletion)
		}
	}
}
