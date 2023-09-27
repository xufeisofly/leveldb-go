package leveldb

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
	return nil
}

func (m *MemTable) Add(seq SequenceNumber, t ValueType, key, value []byte) error {
	return nil
}

func (m *MemTable) Get(key *LookupKey, value *string) error {
	return nil
}
