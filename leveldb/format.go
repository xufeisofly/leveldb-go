package leveldb

type blockContents struct {
	data          []byte // actual contents of data
	cachable      bool   // true if data can be cached
	heapAllocated bool   // true if caller should delete data
}
