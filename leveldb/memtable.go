package leveldb

import "github.com/xufeisofly/leveldb-go/util"

type MemTable struct {
	table      skiplist
	comparator Comparator
}

func NewMemTable(comparator Comparator) *MemTable {
	return &MemTable{
		table:      *NewSkiplist(comparator),
		comparator: comparator,
	}
}

func (m *MemTable) NewIterator() Iterator {
	return NewMemTableIterator(&m.table)
}

// Add puts key value into memtable
// format of an entry
// key_size: varint of internal_key.size()
// key bytes: user key
// tag: sequence + type
// value_size: varint of value.size()
// value bytes: value
func (m *MemTable) Add(seq SequenceNumber, t ValueType, key, value []byte) error {
	internal_key_size := len(key) + TagSize // internal key size
	val_size := len(value)
	internal_key_size_bytes := util.EncodeUvarint(uint64(internal_key_size))
	val_size_bytes := util.EncodeUvarint(uint64(val_size))
	tag_bytes := util.EncodeUint64Fixed(PackSequenceAndType(seq, t))

	buf := make([]byte, len(internal_key_size_bytes)+internal_key_size+len(val_size_bytes)+val_size)

	var i int
	for _, bs := range [][]byte{internal_key_size_bytes, key, tag_bytes, val_size_bytes, value} {
		i += copy(buf[i:], bs)
	}

	m.table.Insert(buf)
	return nil
}

// Get gets value by LookupKey
func (m *MemTable) Get(key *LookupKey) ([]byte, error) {
	memkey := key.MemTableKey()
	tableIter := NewSkiplistIterator(&m.table)
	tableIter.Seek(memkey)
	if tableIter.Valid() {
		// entry format is:
		// internal key length: varint
		// userkey: []byte
		// tag: uint64
		// value length: varint
		// value: []byte
		entry := tableIter.Key()
		ikey, ikeyLen, ikeyLenSize := util.GetVarLengthPrefixedBytes(entry)
		ukey := ikey[:len(ikey)-TagSize]

		if m.comparator.Compare(ukey, key.UserKey()) == 0 {
			tag := util.DecodeUint64Fixed(ikey[len(ikey)-TagSize:])
			_, t := UnpackSequenceAndType(tag)
			switch t {
			case ValueType_Value:
				val, _, _ := util.GetVarLengthPrefixedBytes(entry[ikeyLenSize+int(ikeyLen):])
				return val, nil
			case ValueType_Deletion:
				return nil, Error(Code_NotFound, "")
			}
		}
	}

	return nil, Error(Code_NotFound, "")
}

// memTableIterator
type memTableIterator struct {
	tableIter *skiplistIterator
	tmp       []byte
}

var _ Iterator = (*memTableIterator)(nil)

func NewMemTableIterator(table *skiplist) *memTableIterator {
	tableIter := NewSkiplistIterator(table)
	return &memTableIterator{
		tableIter: tableIter,
		tmp:       nil,
	}
}

func (mi *memTableIterator) Valid() bool {
	return mi.tableIter.Valid()
}

func (mi *memTableIterator) SeekToFirst() {
	mi.tableIter.SeekToFirst()
}

func (mi *memTableIterator) SeekToLast() {
	mi.tableIter.SeekToLast()
}

func (mi *memTableIterator) Seek(target []byte) {
	// encode target size in front of target data before seeking it in the skiplist
	mi.Seek(append(util.EncodeUvarint(uint64(len(target))), target...))
}

func (mi *memTableIterator) Next() {
	mi.Next()
}

func (mi *memTableIterator) Prev() {
	mi.Prev()
}

func (mi *memTableIterator) Key() []byte {
	key, _, _ := util.GetVarLengthPrefixedBytes(mi.Key())
	return key
}

func (mi *memTableIterator) Value() []byte {
	key, l, lSize := util.GetVarLengthPrefixedBytes(mi.Key())
	value, _, _ := util.GetVarLengthPrefixedBytes(key[int(l)+lSize:])
	return value
}
