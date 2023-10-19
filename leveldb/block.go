package leveldb

import "github.com/xufeisofly/leveldb-go/util"

type block struct {
	data          []byte
	size          int
	restartOffset uint64 // offset in data of restart array
	owned         bool   // block owns data
}

func NewBlock(contents *blockContents) *block {
	data := contents.data
	size := len(contents.data)
	owned := contents.heapAllocated
	var restartOffset uint64

	if size < Uint64Size {
		size = 0
	} else {
		maxRestartsAllowed := int((size - Uint64Size) / Uint64Size)
		numRestarts := util.DecodeUint64Fixed(data[(size - Uint64Size):])
		if numRestarts > uint64(maxRestartsAllowed) {
			size = 0
		} else {
			restartOffset = uint64(size) - (1+numRestarts)*Uint64Size
		}
	}

	return &block{
		data:          data,
		size:          size,
		restartOffset: restartOffset,
		owned:         owned,
	}
}

func (b *block) NumRestarts() uint64 {
	if b.size < Uint64Size {
		panic("block size < Uint64Size")
	}
	return util.DecodeUint64Fixed(b.data[(b.size - Uint64Size):])
}

func (b *block) Size() int {
	return b.size
}

func (b *block) NewIterator(comp Comparator) Iterator {
	if b.size < Uint64Size {
		return NewEmptyIterator()
	}
	numRestarts := b.NumRestarts()
	if numRestarts == 0 {
		return NewEmptyIterator()
	}
	return NewBlockIterator(comp, b.data, b.restartOffset, numRestarts)
}

type blockIter struct {
	comparator  Comparator
	data        []byte // underlying block contents
	restarts    uint64 // offset of restart array
	numRestarts uint64 // number of uint64 entries in restart array

	current      uint64 // current is offset in data of current entry. >= restarts if !Valid
	restartIndex uint64 // index of restart block in which current falls
	key          []byte
	value        []byte
	valueOffset  uint64 // offset of value of current entry
}

var _ Iterator = (*blockIter)(nil)

func NewBlockIterator(comp Comparator, data []byte, restarts, numRestarts uint64) *blockIter {
	return &blockIter{
		comparator:  comp,
		data:        data,
		restarts:    restarts,
		numRestarts: numRestarts,

		current:      restarts,
		restartIndex: numRestarts,
		key:          []byte{},
		value:        []byte{},
		valueOffset:  0,
	}
}

func (biter *blockIter) compare(a, b []byte) int8 {
	return biter.comparator.Compare(a, b)
}

func (biter *blockIter) nextEntryOffset() uint64 {
	return biter.valueOffset + uint64(len(biter.value))
}

func (biter *blockIter) getRestartPoint(index uint64) uint64 {
	if index >= biter.numRestarts {
		panic("index >= biter.numRestarts")
	}
	return util.DecodeUint64Fixed(biter.data[biter.restarts+index*Uint64Size:])
}

func (biter *blockIter) seekToRestartPoint(index uint64) {
	biter.key = []byte{}
	biter.restartIndex = index
	// current will be fixed by parseNextKey()

	// parseNextKey starts at the end of value, so set value accordingly
	offset := biter.getRestartPoint(index)
	biter.valueOffset = offset
	biter.value = []byte{}
}

func (biter *blockIter) Valid() bool {
	return biter.current < biter.restarts
}

func (biter *blockIter) Key() []byte {
	if !biter.Valid() {
		panic("block iterator is invalid")
	}
	return biter.key
}

func (biter *blockIter) Value() []byte {
	if !biter.Valid() {
		panic("block iterator is invalid")
	}
	return biter.value
}

func (biter *blockIter) Next() {
	if !biter.Valid() {
		panic("block iterator is invalid")
	}
	biter.parseNextKey()
}

func (biter *blockIter) Prev() {
	if !biter.Valid() {
		panic("block iterator is invalid")
	}

	// Scan backwards to a restart point before current
	original := biter.current
	for biter.getRestartPoint(biter.restartIndex) >= original {
		if biter.restartIndex == 0 {
			// no more entries
			biter.current = biter.restarts
			biter.restartIndex = biter.numRestarts
			return
		}
		biter.restartIndex -= 1
	}

	biter.seekToRestartPoint(biter.restartIndex)
	for {
		ok, _ := biter.parseNextKey()
		if !ok {
			return
		}
		if biter.nextEntryOffset() >= original {
			return
		}
	}
}

func (biter *blockIter) Seek(target []byte) {
	// binary search in restart array to find the last restart point
	// with a key < target
	left, right := uint64(0), biter.numRestarts-1
	currentKeyComp := 0

	if biter.Valid() {
		// If we are already scanning, use the current position as a starting
		// point. This is beneficial if the key we are seeking to is ahead of the current position.
		currentKeyComp = int(biter.compare(biter.key, target))
		if currentKeyComp < 0 {
			// key is smaller than target
			left = biter.restartIndex
		} else if currentKeyComp > 0 {
			// key is larger than target
			right = biter.restartIndex
		} else {
			// we are seeking to the key we are already at
			return
		}
	}

	for left < right {
		mid := (left + right + 1) >> 1
		regionOffset := biter.getRestartPoint(mid)
		var shared, nonShared, valueLength uint64
		l, err := DecodeEntry(biter.data[regionOffset:biter.restarts], &shared, &nonShared, &valueLength)
		if err != nil || l == 0 || shared != 0 {
			biter.corruptionError()
			return
		}
		midKey := biter.data[regionOffset+l : regionOffset+l+nonShared]
		if biter.compare(midKey, target) < 0 {
			// key at "mid" is smaller than "target", therefore all
			// blocks before "mid" are uninteresting
			left = mid
		} else {
			// key at "mid" is >= "target", therefore all blocks
			// at or after "mid" are uninteresting
			right = mid - 1
		}
	}

	// We might be able to use our current position within the restart block.
	// This is true if we determined the key we desire is in the current block
	// and is after than the current key.
	if currentKeyComp != 0 && !biter.Valid() {
		panic("block iterator is invalid")
	}

	skipSeek := left == biter.restartIndex && currentKeyComp < 0
	if !skipSeek {
		biter.seekToRestartPoint(left)
	}
	// linear search for first key >= target
	for {
		if ok, _ := biter.parseNextKey(); !ok {
			return
		}
		if biter.compare(biter.key, target) >= 0 {
			return
		}
	}
}

func (biter *blockIter) SeekToFirst() {
	biter.seekToRestartPoint(0)
	biter.parseNextKey()
}

func (biter *blockIter) SeekToLast() {
	biter.seekToRestartPoint(biter.numRestarts - 1)
	for {
		if ok, _ := biter.parseNextKey(); !ok {
			return
		}
		if biter.nextEntryOffset() >= biter.restarts {
			return
		}
	}
}

func (biter *blockIter) parseNextKey() (bool, error) {
	biter.current = biter.nextEntryOffset()
	p := biter.current
	limit := biter.restarts
	if p >= limit {
		// no more entries to return, mark as invalid
		biter.current = biter.restarts
		biter.restartIndex = biter.numRestarts
		return false, Error(Code_Corruption, "")
	}

	// decode next entry
	var shared, nonShared, valueLength uint64
	l, err := DecodeEntry(biter.data[p:limit], &shared, &nonShared, &valueLength)
	if err != nil || l == 0 || len(biter.key) < int(shared) {
		return false, biter.corruptionError()
	}
	biter.key = biter.key[:shared]
	biter.key = append(biter.key, biter.data[p+l:p+l+nonShared]...)
	biter.valueOffset = p + l + nonShared
	biter.value = biter.data[biter.valueOffset : biter.valueOffset+valueLength]

	for biter.restartIndex+1 < biter.numRestarts && biter.getRestartPoint(biter.restartIndex+1) < biter.current {
		biter.restartIndex += 1
	}

	return true, nil
}

func (biter *blockIter) corruptionError() error {
	biter.current = biter.restarts
	biter.restartIndex = biter.numRestarts
	biter.key = []byte{}
	biter.value = []byte{}
	return Error(Code_Corruption, "")
}

// DecodeEntry decodes entry from data bytes
// returns length of the three decoded values
func DecodeEntry(data []byte, shared, nonShared, valueLength *uint64) (uint64, error) {
	var l uint64
	if len(data) < 3 {
		return l, Error(Code_NotFound, "")
	}

	*shared = uint64(data[0])
	*nonShared = uint64(data[1])
	*valueLength = uint64(data[2])
	if (*shared | *nonShared | *valueLength) < 128 {
		// fast path: all three values are encoded in one byte each
		l += 3
	} else {
		s, sLength := util.DecodeUvarint(data)
		if sLength == 0 {
			return l, Error(Code_NotFound, "")
		}
		ns, nsLength := util.DecodeUvarint(data[sLength:])
		if nsLength == 0 {
			return l, Error(Code_NotFound, "")
		}
		vl, vlLength := util.DecodeUvarint(data[sLength+nsLength:])
		if vlLength == 0 {
			return l, Error(Code_NotFound, "")
		}

		*shared = s
		*nonShared = ns
		*valueLength = vl
		l = uint64(sLength) + uint64(nsLength) + uint64(vlLength)
	}

	// rest of data length is not enough for kv data
	if len(data)-int(l) < int(*nonShared+*valueLength) {
		return l, Error(Code_NotFound, "")
	}
	return l, nil
}
