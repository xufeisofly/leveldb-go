package leveldb

import (
	"hash/crc32"

	"github.com/xufeisofly/leveldb-go/util"
)

type tableBuilder struct {
	options           *Options
	indexBlockOptions *Options
	file              WritableFile
	offset            uint64
	dataBlock         *blockBuilder
	indexBlock        *blockBuilder
	lastKey           []byte
	numEntries        uint64
	closed            bool // either Finish() or Abandon() has been called
	filterBlock       *filterBlockBuilder

	// We do not emit the index entry for a block until we have seen the
	// first key for the next data block.  This allows us to use shorter
	// keys in the index block.  For example, consider a block boundary
	// between the keys "the quick brown fox" and "the who".  We can use
	// "the r" as the key for the index block entry since it is >= all
	// entries in the first block and < all entries in subsequent
	// blocks.
	//
	// Invariant: r.pendingIndexEntry is true only if data_block is empty.
	pendingIndexEntry bool
	pendingHandle     *blockHandle // Handle to add to index block

	compressedOutput []byte
}

func NewTableBuilder(opt *Options, f WritableFile) *tableBuilder {
	var filterBlock *filterBlockBuilder
	if opt.FilterPolicy != nil {
		filterBlock = NewFilterBlockBuilder(opt.FilterPolicy)
	}
	tb := &tableBuilder{
		options:           opt,
		indexBlockOptions: opt,
		file:              f,
		offset:            0,
		dataBlock:         NewBlockBuilder(opt),
		indexBlock:        NewBlockBuilder(opt),
		numEntries:        0,
		closed:            false,
		filterBlock:       filterBlock,
		pendingIndexEntry: false,
	}
	tb.indexBlockOptions.BlockRestartInternal = 1
	if tb.filterBlock != nil {
		tb.filterBlock.StartBlock(0)
	}
	return tb
}

func (b *tableBuilder) ChangeOptions(options *Options) error {
	// Note: if more fields are added to Options, update
	// this function to catch changes that should not be allowed to
	// change in the middle of building a Table.
	if options.Comparator != b.options.Comparator {
		return Error(Code_InvalidArgument, "changing comparator while building table")
	}

	// Note that any live BlockBuilders point to b.options and therefore
	// will automatically pick up the updated options.
	b.options = options
	b.indexBlockOptions = options
	b.indexBlockOptions.BlockRestartInternal = 1
	return nil
}

func (b *tableBuilder) Add(key, value []byte) error {
	if b.closed {
		return Error(Code_Corruption, "")
	}
	if b.numEntries > 0 {
		if b.options.Comparator.Compare(key, b.lastKey) <= 0 {
			return Error(Code_Corruption, "")
		}
	}

	if b.pendingIndexEntry {
		if !b.dataBlock.empty() {
			return Error(Code_Corruption, "")
		}
		b.options.Comparator.FindShortestSeparator(&b.lastKey, key)
		handleEncoding := make([]byte, 0)
		b.pendingHandle.EncodeTo(&handleEncoding)
		if err := b.indexBlock.Add(b.lastKey, handleEncoding); err != nil {
			return err
		}
		b.pendingIndexEntry = false
	}

	if b.filterBlock != nil {
		b.filterBlock.AddKey(key)
	}

	b.lastKey = key
	b.numEntries++
	b.dataBlock.Add(key, value)

	if b.dataBlock.CurrentSizeEstimate() >= b.options.BlockSize {
		if err := b.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (b *tableBuilder) Flush() error {
	if b.closed {
		return Error(Code_Corruption, "")
	}
	if b.dataBlock.empty() {
		return nil
	}
	if b.pendingIndexEntry {
		return Error(Code_Corruption, "pendingIndexEntry is true")
	}
	err := b.writeBlock(b.dataBlock, b.pendingHandle)
	if err != nil {
		return err
	}
	b.pendingIndexEntry = true
	if err := b.file.Flush(); err != nil {
		return err
	}

	if b.filterBlock != nil {
		if err := b.filterBlock.StartBlock(b.offset); err != nil {
			return err
		}
	}
	return nil
}

func (b *tableBuilder) writeBlock(block *blockBuilder, handle *blockHandle) error {
	// File format contains a sequence of blocks where each blocks has:
	// blockData: uint8[n]
	// type: uint8
	// crc: uint32
	raw := block.Finish()

	var blockContents []byte
	typ := b.options.Compression
	// TODO(postrelease): Support more compression options: zlib?
	switch {
	case typ == CompressionType_NoCompression:
		blockContents = raw
	case typ == CompressionType_Snappy:
		compressed := b.compressedOutput
		if SnappyCompress(raw, &compressed) && len(compressed) < len(raw)-len(raw)/8 {
			blockContents = compressed
		} else {
			// Snappy not supported, or compressed less than 12.5%, so just store uncompressed form
			blockContents = raw
			typ = CompressionType_NoCompression
		}
	case typ == CompressionType_Zstd:
		compressed := b.compressedOutput
		if ZstdCompress(raw, &compressed) && len(compressed) < len(raw)-len(raw)/8 {
			blockContents = compressed
		} else {
			// Snappy not supported, or compressed less than 12.5%, so just store uncompressed form
			blockContents = raw
			typ = CompressionType_NoCompression
		}
	}
	if err := b.writeRawBlock(blockContents, typ, handle); err != nil {
		return err
	}
	b.compressedOutput = []byte{}
	block.Reset()

	return nil
}

func (b *tableBuilder) writeRawBlock(blockContents []byte, typ CompressionType, handle *blockHandle) error {
	handle.SetOffset(b.offset)
	handle.SetSize(uint64(len(blockContents)))
	err := b.file.Append(blockContents)
	if err != nil {
		return err
	}

	trailer := make([]byte, kBlockTrailerSize)
	typB := byte(typ)
	trailer[0] = typB
	// crc cover block type
	crc := crc32.ChecksumIEEE(append(blockContents, typB))
	r := util.EncodeUint32Fixed(util.Mask(crc))
	copy(trailer[1:], r)

	if err := b.file.Append(trailer); err != nil {
		return err
	}

	b.offset += uint64(len(blockContents) + kBlockTrailerSize)

	return nil
}

func (b *tableBuilder) Finish() error {
	if err := b.Flush(); err != nil {
		return err
	}
	if b.closed {
		return Error(Code_Corruption, "")
	}
	b.closed = true

	var filterBlockHandle, metaindexBlockHandle, indexBlockHandle blockHandle

	// Write filter block
	if b.filterBlock != nil {
		err := b.writeRawBlock(b.filterBlock.Finish(), CompressionType_NoCompression, &filterBlockHandle)
		if err != nil {
			return err
		}
	}

	// Write metaindex block
	metaIndexBlock := NewBlockBuilder(b.options)
	if b.filterBlock != nil {
		// Add mapping from "filter.Name" to location of filter data
		key := []byte("filter.")
		key = append(key, []byte(b.options.FilterPolicy.Name())...)
		handleEncoding := make([]byte, 0)
		filterBlockHandle.EncodeTo(&handleEncoding)
		metaIndexBlock.Add(key, handleEncoding)
	}

	err := b.writeBlock(metaIndexBlock, &metaindexBlockHandle)
	if err != nil {
		return err
	}

	// TODO Write footer

	return nil
}

func (b *tableBuilder) Abandon() error {
	return nil
}

func (b *tableBuilder) NumEntries() uint64 {
	return b.numEntries
}

func (b *tableBuilder) FileSize() uint64 {
	return b.offset
}
