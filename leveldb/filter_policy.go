package leveldb

// A database can be configured with a custom FilterPolicy object.
// This object is responsible for creating a small filter from a set
// of keys.  These filters are stored in leveldb and are consulted
// automatically by leveldb to decide whether or not to read some
// information from disk. In many cases, a filter can cut down the
// number of disk seeks form a handful to a single disk seek per
// DB::Get() call.
//
// Most people will want to use the builtin bloom filter support (see
// NewBloomFilterPolicy() below).

type FilterPolicy interface {
	// Return the name of this policy.  Note that if the filter encoding
	// changes in an incompatible way, the name returned by this method
	// must be changed.  Otherwise, old incompatible filters may be
	// passed to methods of this type.
	Name() string

	// keys[0,n-1] contains a list of keys (potentially with duplicates)
	// that are ordered according to the user supplied comparator.
	// Append a filter that summarizes keys[0,n-1] to *dst.
	//
	// Warning: do not change the initial contents of *dst.  Instead,
	// append the newly constructed filter to *dst.
	CreateFilter(keys [][]byte, dst *[]byte)

	// "filter" contains the data appended by a preceding call to
	// CreateFilter() on this class.  This method must return true if
	// the key was in the list of keys passed to CreateFilter().
	// This method may return true or false if the key was not on the
	// list, but it should aim to return false with a high probability.
	KeyMayMatch(key []byte, filter []byte) bool
}
