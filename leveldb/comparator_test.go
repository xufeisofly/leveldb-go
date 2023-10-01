package leveldb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xufeisofly/leveldb-go/leveldb"
)

// TODO test BytewiseComparator

func IKey(ukey []byte, seq leveldb.SequenceNumber, t leveldb.ValueType) []byte {
	return leveldb.DumpInternalKey(leveldb.NewParsedInternalKey(ukey, seq, t))
}

func Shorten(s []byte, l []byte) []byte {
	leveldb.NewInternalKeyComparator(leveldb.NewBytewiseComparator()).FindShortestSeparator(&s, l)
	return s
}

func ShortSuccessor(s []byte) []byte {
	leveldb.NewInternalKeyComparator(leveldb.NewBytewiseComparator()).FindShortSuccessor(&s)
	return s
}

func TestInternalKeyComparator_ShortSeparator(t *testing.T) {
	// when user keys are same
	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("foo"), 99, leveldb.ValueType_Value)))

	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("foo"), 101, leveldb.ValueType_Value)))

	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("foo"), 100, leveldb.ValueType_Value)))

	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("foo"), 100, leveldb.ValueType_Deletion)))

	// when user keys are misordered
	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("bar"), 99, leveldb.ValueType_Value)))

	// when user keys are different, but correctly ordered
	assert.Equal(t,
		IKey([]byte("g"), leveldb.KMaxSequenceNumber, leveldb.ValueType_ForSeek),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("hello"), 200, leveldb.ValueType_Value)))

	// when start user key is prefix of limit user key
	assert.Equal(t,
		IKey([]byte("foo"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foo"), 100, leveldb.ValueType_Value), IKey([]byte("foobar"), 200, leveldb.ValueType_Value)))

	// when limit user key is prefix of start user key
	assert.Equal(t,
		IKey([]byte("foobar"), 100, leveldb.ValueType_Value),
		Shorten(IKey([]byte("foobar"), 100, leveldb.ValueType_Value), IKey([]byte("foo"), 200, leveldb.ValueType_Value)))
}

func TestInternalKeyComparator_ShortestSuccessor(t *testing.T) {
	assert.Equal(t,
		IKey([]byte("g"), leveldb.KMaxSequenceNumber, leveldb.ValueType_ForSeek),
		ShortSuccessor(IKey([]byte("foo"), 100, leveldb.ValueType_Value)))
	assert.Equal(t,
		IKey([]byte("\xff\xff"), 100, leveldb.ValueType_Value),
		ShortSuccessor(IKey([]byte("\xff\xff"), 100, leveldb.ValueType_Value)))
}
