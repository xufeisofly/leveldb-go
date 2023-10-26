package leveldb

import (
	"github.com/xufeisofly/leveldb-go/util"
)

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
type filterBlockBuilder struct {
	policy FilterPolicy
	keys   [][]byte // flattened key contents
	// start         []uint64 // starting index in keys of each key
	result []byte // filter data computed so far
	// tmpKeys       [][]byte // policy.CreateFilter() argument
	filterOffsets []uint64
}

const kFilterBaseLg = 11

var kFilterBase int = 1 << kFilterBaseLg

func NewFilterBlockBuilder(policy FilterPolicy) *filterBlockBuilder {
	return &filterBlockBuilder{
		policy: policy,
	}
}

func (fb *filterBlockBuilder) StartBlock(blockOffset uint64) error {
	filterIdx := blockOffset / uint64(kFilterBase)
	if filterIdx < uint64(len(fb.filterOffsets)) {
		return Error(Code_Corruption, "filterIdx < filterOffsets size")
	}
	for filterIdx > uint64(len(fb.filterOffsets)) {
		fb.generateFilter()
	}
	return nil
}

func (fb *filterBlockBuilder) AddKey(key []byte) {
	// fb.start = append(fb.start, uint64(len(fb.keys)))
	fb.keys = append(fb.keys, key)
}

func (fb *filterBlockBuilder) Finish() []byte {
	if len(fb.keys) != 0 {
		fb.generateFilter()
	}

	// Append array of per-filter offsets
	arrayOffset := len(fb.result)
	for i := 0; i < len(fb.filterOffsets); i++ {
		util.PutUint64Fixed(&fb.result, fb.filterOffsets[i])
	}

	util.PutUint64Fixed(&fb.result, uint64(arrayOffset))
	fb.result = append(fb.result, kFilterBaseLg)
	return fb.result
}

// TODO tmpKeys 和 start 没什么用，c++ 代码中是因为 keys 是 []byte，即平铺后的，需要先解码
func (fb *filterBlockBuilder) generateFilter() {
	numKeys := len(fb.keys)
	if numKeys == 0 {
		fb.filterOffsets = append(fb.filterOffsets, uint64(len(fb.result)))
		return
	}

	fb.filterOffsets = append(fb.filterOffsets, uint64(len(fb.result)))
	fb.policy.CreateFilter(fb.keys, &fb.result)

	fb.keys = [][]byte{}
}

type filterBlockReader struct {
	policy FilterPolicy
	data   []byte // filter data at block start
	offset []byte // offset array at block end
	num    int    // number of entries in offset array
	baseLg int    // encoding parameter
}

func NewFilterBlockReader(policy FilterPolicy, contents []byte) *filterBlockReader {
	n := len(contents)
	if n < 9 {
		// 1 byte for baseLg and 8 for start of offset array
		return nil
	}
	baseLg := contents[n-1]
	lastWord := util.DecodeUint64Fixed(contents[n-9:]) // length of filter data
	if lastWord > uint64(n-9) {
		return nil
	}
	data := contents[:lastWord]
	offset := data[lastWord : n-1]
	num := (n - 9 - int(lastWord)) / 8 // num of filters

	return &filterBlockReader{
		policy: policy,
		data:   data,
		offset: offset,
		num:    num,
		baseLg: int(baseLg),
	}
}

func (fr *filterBlockReader) KeyMayMatch(blockOffset uint64, key []byte) bool {
	index := blockOffset >> uint64(fr.baseLg)
	if index < uint64(fr.num) {
		start := util.DecodeUint64Fixed(fr.offset[8*index:])
		limit := util.DecodeUint64Fixed(fr.offset[8*index+8:])
		if start <= limit && limit <= uint64(len(fr.data)) {
			filter := fr.data[start:limit]
			return fr.policy.KeyMayMatch(key, filter)
		} else if start == limit {
			// Empty filters do not match any keys
			return false
		}
	}
	// Errors are treated as potential matches
	return true
}
