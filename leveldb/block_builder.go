package leveldb

import (
	"github.com/xufeisofly/leveldb-go/util"
)

// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint64
//     unshared_bytes: varint64
//     value_length: varint64
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint64[num_restarts]
//     num_restarts: uint64
// restarts[i] contains the offset within the block of the ith restart point.

type blockBuilder struct {
	options  *Options
	buffer   []byte   // Destination buffer
	restarts []uint32 // Restart points
	counter  int      // Number of entries emitted since restart
	finished bool     // Has Finished been called?
	lastKey  []byte
}

func NewBlockBuilder(options *Options) *blockBuilder {
	return &blockBuilder{
		options:  options,
		buffer:   make([]byte, 0),
		restarts: []uint32{0}, // first restart point is at offset 0
		counter:  0,
		finished: false,
		lastKey:  make([]byte, 0),
	}
}

// Reset the contents as if the BlockBuilder was just constructed
func (bb *blockBuilder) Reset() {
	bb.buffer = make([]byte, 0)
	bb.restarts = []uint32{0}
	bb.counter = 0
	bb.finished = false
	bb.lastKey = make([]byte, 0)
}

// REQUIRES: Finish() has not been called since the last call to Reset()
// REQUIRES: key is larger than any previous added key
func (bb *blockBuilder) Add(key, value []byte) error {
	if bb.finished {
		return Error(Code_Corruption, "")
	}
	if bb.counter > bb.options.BlockRestartInternal {
		return Error(Code_Corruption, "")
	}
	if len(bb.buffer) != 0 && bb.options.Comparator.Compare(key, bb.lastKey) <= 0 {
		// imported from immutable memtable, blockbuilder add is always ordered
		// so key is always larger than last key
		return Error(Code_Corruption, "")
	}
	var shared int

	if bb.counter < bb.options.BlockRestartInternal {
		// See how much sharing to do with previous string
		minLength := util.Min[int](len(bb.lastKey), len(key))
		for shared < minLength && bb.lastKey[shared] == key[shared] {
			shared++
		}
	} else {
		// restart compression
		bb.restarts = append(bb.restarts, uint32(len(bb.buffer)))
		bb.counter = 0
	}

	nonShared := len(key) - shared

	// Add "<shared><nonShared><valueSize>" to buffer
	util.PutUvarint(&bb.buffer, uint64(shared))
	util.PutUvarint(&bb.buffer, uint64(nonShared))
	util.PutUvarint(&bb.buffer, uint64(len(value)))

	// Add "string delta to buffer followed by value"
	bb.buffer = append(bb.buffer, key[shared:]...)
	bb.buffer = append(bb.buffer, value...)

	// update state
	bb.lastKey = bb.lastKey[:shared]
	bb.lastKey = append(bb.lastKey, key[shared:]...)

	if bb.options.Comparator.Compare(bb.lastKey, key) != 0 {
		return Error(Code_Corruption, "")
	}
	bb.counter++

	return nil
}

// Finish building the block and return a bytearray that refers to the
// block contents.  The returned bytearray will remain valid for the
// lifetime of this builder or until Reset() is called.
func (bb *blockBuilder) Finish() []byte {
	// add <restart1><restart2>...<restartn><restartsNumber>
	for _, restart := range bb.restarts {
		util.PutUint64Fixed(&bb.buffer, uint64(restart))
	}
	util.PutUint64Fixed(&bb.buffer, uint64(len(bb.restarts)))
	bb.finished = true
	return bb.buffer
}

// Returns an estimate of the current (uncompressed) size of the block
// we are building.
func (bb *blockBuilder) CurrentSizeEstimate() uint64 {
	return uint64(len(bb.buffer) + // Raw data buffer
		len(bb.restarts)*Uint64Size + // Restart array
		Uint64Size) // Restart array length
}

func (bb *blockBuilder) empty() bool {
	return len(bb.buffer) == 0
}
