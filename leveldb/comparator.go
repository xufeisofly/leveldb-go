package leveldb

import (
	"bytes"

	"github.com/xufeisofly/leveldb-go/util"
)

type Comparator interface {
	Compare(a, b []byte) int8
	Name() string
	FindShortestSeparator(start *[]byte, limit []byte) error
	FindShortSuccessor(key *[]byte) error
}

type bytewiseComparator struct{}

var _ Comparator = (*bytewiseComparator)(nil)

func NewBytewiseComparator() *bytewiseComparator {
	return &bytewiseComparator{}
}

func (c *bytewiseComparator) Compare(a, b []byte) int8 {
	return int8(bytes.Compare(a, b))
}

func (c *bytewiseComparator) Name() string {
	return "leveldb.BytewiseComparator"
}

// FindShortestSeparator shorten the start physically if *start < limit
// eg.
// *start: helloWorld
// limit: helloZookeeper
// because *start < limit, start -> helloX
func (c *bytewiseComparator) FindShortestSeparator(start *[]byte, limit []byte) error {
	minLen := util.Min(len(*start), len(limit))
	// Find length of common prefix
	var diffIdx int
	for diffIdx < minLen && (*start)[diffIdx] == limit[diffIdx] {
		diffIdx++
	}

	if diffIdx >= minLen {
		// Do not shorten if one string is a prefix of the other
		return nil
	} else {
		diffByte := (*start)[diffIdx]
		if diffByte < 0xff && diffByte+1 < limit[diffIdx] {
			(*start)[diffIdx]++
			*start = (*start)[:diffIdx+1]
			if c.Compare(*start, limit) >= 0 {
				return Error(Code_Corruption, "start >= limit")
			}
		}
	}
	return nil
}

// FindShortSuccessor change key to the shortest larger bytes
func (c *bytewiseComparator) FindShortSuccessor(key *[]byte) error {
	for i := range *key {
		if (*key)[i] < 0xff {
			(*key)[i]++
			*key = (*key)[:i+1]
			return nil
		}
	}
	return nil
}

type internalKeyComparator struct {
	comparator Comparator
}

var _ Comparator = (*internalKeyComparator)(nil)

func NewInternalKeyComparator(c Comparator) *internalKeyComparator {
	return &internalKeyComparator{c}
}

// Compare by UserKey
// Order By:
// increasing user key(according to the user-supplied comparator)
// decreasing sequence number
// decreasing type(though sequence should be enough to disambiguate)
func (ic *internalKeyComparator) Compare(a, b []byte) int8 {
	r := ic.comparator.Compare(ExtractUserKey(a), ExtractUserKey(b))
	if r == 0 {
		taga := util.DecodeUint64Fixed(a[len(a)-TagSize:])
		tagb := util.DecodeUint64Fixed(b[len(b)-TagSize:])
		if taga > tagb {
			r = -1
		} else if taga < tagb {
			r = +1
		}
	}
	return r
}

func (ic *internalKeyComparator) Name() string {
	return "leveldb.InternalKeyComparator"
}

// FindShortestSeparator shorten the internal key(start) by user-supplied comparator
// only the user key portion
func (ic *internalKeyComparator) FindShortestSeparator(start *[]byte, limit []byte) error {
	userStart := ExtractUserKey(*start)
	userLimit := ExtractUserKey(limit)
	userStartShort := make([]byte, len(userStart))
	copy(userStartShort, userStart)

	ic.comparator.FindShortestSeparator(&userStartShort, userLimit)
	// if userStart is successfully shorten
	if len(userStartShort) < len(userStart) && ic.comparator.Compare(userStartShort, userStart) > 0 {
		userStartShort = append(userStartShort, util.EncodeUint64Fixed(PackSequenceAndType(KMaxSequenceNumber, ValueType_ForSeek))...)
		if ic.comparator.Compare(*start, userStartShort) >= 0 {
			return Error(Code_Corruption, "")
		}
		if ic.comparator.Compare(userStartShort, limit) >= 0 {
			return Error(Code_Corruption, "")
		}
		*start = userStartShort
	}
	return nil
}

func (ic *internalKeyComparator) FindShortSuccessor(key *[]byte) error {
	ukey := ExtractUserKey(*key)
	tmp := make([]byte, len(ukey))
	copy(tmp, ukey)
	ic.comparator.FindShortSuccessor(&tmp)

	if len(tmp) < len(ukey) && ic.comparator.Compare(ukey, tmp) < 0 {
		tmp = append(tmp, util.EncodeUint64Fixed(PackSequenceAndType(KMaxSequenceNumber, ValueType_ForSeek))...)
		if ic.Compare(*key, tmp) >= 0 {
			return Error(Code_Corruption, "")
		}
		*key = tmp
	}
	return nil
}
