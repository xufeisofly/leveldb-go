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

func TestInternalKey_DumpAndParse(t *testing.T) {
	ukey := []byte("abc")
	seq := leveldb.SequenceNumber(99)
	valueType := leveldb.ValueType_Value
	parsedIKey := leveldb.NewParsedInternalKey(ukey, seq, valueType)

	ikeyBytes := leveldb.DumpInternalKey(parsedIKey)
	ret, err := leveldb.ParseInternalKey(ikeyBytes)
	assert.NoError(t, err)
	assert.Equal(t, parsedIKey.UserKey, ret.UserKey)
	assert.Equal(t, parsedIKey.Sequence, ret.Sequence)
	assert.Equal(t, parsedIKey.Type, ret.Type)
}
