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
