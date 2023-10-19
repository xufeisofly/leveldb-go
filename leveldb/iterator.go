package leveldb

// Iterator
type Iterator interface {
	Valid() bool
	// Position at the first key, the iterator is Valid if the source is not empty
	SeekToFirst()
	// Position at the last key, the iterator is Valid if the source is not empty
	SeekToLast()
	// Position at the first key that is at or past target
	Seek(target []byte)
	// Move to the next entry
	Next()
	// Move to the previous entry
	Prev()
	// Return the key for the current entry
	Key() []byte
	// Return the value for the current entry
	Value() []byte
}

type emptyIterator struct{}

func NewEmptyIterator() *emptyIterator {
	return &emptyIterator{}
}

var _ Iterator = (*emptyIterator)(nil)

func (i *emptyIterator) Valid() bool {
	return false
}
func (i *emptyIterator) SeekToFirst()       {}
func (i *emptyIterator) SeekToLast()        {}
func (i *emptyIterator) Seek(target []byte) {}
func (i *emptyIterator) Next()              {}
func (i *emptyIterator) Prev()              {}
func (i *emptyIterator) Key() []byte {
	panic("invalid")
}
func (i *emptyIterator) Value() []byte {
	panic("invalid")
}
