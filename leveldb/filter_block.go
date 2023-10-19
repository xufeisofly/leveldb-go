package leveldb

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
type filterBlockBuilder struct {
	policy        FilterPolicy
	keys          []byte   // flattened key contents
	start         []uint64 // starting index in keys of each key
	result        []byte   // filter data computed so far
	tmpKeys       [][]byte // policy.CreateFilter() argument
	filterOffsets []uint64
}
