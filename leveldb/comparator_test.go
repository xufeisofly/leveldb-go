package leveldb_test

import (
	"testing"

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

func TestBytewiseComparator_ShortSeparator(t *testing.T) {

}

// TODO test InternalKeyComparator
