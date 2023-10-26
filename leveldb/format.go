package leveldb

import "github.com/xufeisofly/leveldb-go/util"

type blockContents struct {
	data          []byte // actual contents of data
	cachable      bool   // true if data can be cached
	heapAllocated bool   // true if caller should delete data
}

// blockHandle is a pointer to the extent of a file that stores a data
// block or a meta block
type blockHandle struct {
	offset uint64
	size   uint64
}

// Maximum encoding length of a BlockHandle
const kMaxEncodedLength int = 10 + 10

// 1-byte type + 32-bit crc
const kBlockTrailerSize int = 5

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
const kTableMagicNumber uint64 = 0xdb4775248b80fb57

func NewBlockHandle() *blockHandle {
	return &blockHandle{
		offset: 0,
		size:   0,
	}
}

func (h *blockHandle) Offset() uint64 {
	return h.offset
}

func (h *blockHandle) SetOffset(offset uint64) {
	h.offset = offset
}

func (h *blockHandle) Size() uint64 {
	return h.size
}

func (h *blockHandle) SetSize(size uint64) {
	h.size = size
}

func (h *blockHandle) EncodeTo(dst *[]byte) {
	if h.offset == 0 || h.size == 0 {
		panic("some fields have not been set")
	}
	util.PutUvarint(dst, h.offset)
	util.PutUvarint(dst, h.size)
}

func (h *blockHandle) DecodeFrom(input *[]byte) error {
	if util.GetUvarint(input, &h.offset) && util.GetUvarint(input, &h.size) {
		return nil
	}
	return Error(Code_Corruption, "bad block handle")
}

// footer encapsulates the fixed information stored at the tail
// end of every table file
type footer struct {
	metaindexHandle *blockHandle
	indexHandle     *blockHandle
}

// Encoded length of a footer.  Note that the serialization of a
// footer will always occupy exactly this many bytes.  It consists
// of two block handles and a magic number.
const kEncodedLength = 2*kMaxEncodedLength + 8

func NewFooter() *footer {
	return &footer{}
}

// The block handle for the metaindex block of the table
func (f *footer) MetaindexHandle() *blockHandle { return f.metaindexHandle }

func (f *footer) SetMetaindexHandle(h *blockHandle) {
	f.metaindexHandle = h
}

// The block handle for the index block of the table
func (f *footer) IndexHandle() *blockHandle { return f.indexHandle }

func (f *footer) SetIndexHandle(h *blockHandle) {
	f.indexHandle = h
}

func (f *footer) EncodeTo(dst *[]byte) error {
	originalSize := len(*dst)
	f.metaindexHandle.EncodeTo(dst)
	f.indexHandle.EncodeTo(dst)

	util.PutUint64Fixed(dst, kTableMagicNumber)
	if len(*dst) != originalSize+kEncodedLength {
		return Error(Code_Corruption, "")
	}
	return nil
}

func (f *footer) DecodeFrom(input *[]byte) error {
	if len(*input) < kEncodedLength {
		return Error(Code_Corruption, "not an sstable (footer too short)")
	}

	magic := util.DecodeUint64Fixed((*input)[kEncodedLength-8:])

	if magic != kTableMagicNumber {
		return Error(Code_Corruption, "not an sstable (bad magic number)")
	}

	err := f.metaindexHandle.DecodeFrom(input)
	if err != nil {
		return err
	}
	err = f.indexHandle.DecodeFrom(input)
	if err != nil {
		return err
	}
	// We skip over any leftover data (just padding for now) in "input"
	*input = (*input)[kEncodedLength:]
	return nil
}
