package leveldb

import "github.com/xufeisofly/leveldb-go/util"

type LookupKey struct {
	data             []byte
	internalKeyStart int // the size of internal key length
}

const TagSize = 8
const kMaxSequenceNumber SequenceNumber = ((1 << 56) - 1)

func NewLookupKey(userKey []byte, seq SequenceNumber) *LookupKey {
	usize := len(userKey)
	internalkeyLengthBytes := util.EncodeUvarint(uint64(usize + TagSize))
	tagBytes := util.EncodeUint64Fixed(PackSequenceAndType(seq, ValueType_ForSeek))
	data := append(internalkeyLengthBytes, append(userKey, tagBytes...)...)

	return &LookupKey{
		data:             data,
		internalKeyStart: len(internalkeyLengthBytes),
	}
}

func (lk *LookupKey) MemTableKey() []byte {
	return lk.data
}

func (lk *LookupKey) InternalKey() []byte {
	return lk.data[lk.internalKeyStart:]
}

func (lk *LookupKey) UserKey() []byte {
	return lk.data[lk.internalKeyStart : len(lk.data)-TagSize]
}

// PackSequenceAndType pack sequence number with type to a 8 bytes int
// sequence number(7 bytes) + value type(1 byte)
func PackSequenceAndType(seq SequenceNumber, t ValueType) uint64 {
	if seq > kMaxSequenceNumber {
		panic("sequence number not valid")
	}
	if t > ValueType_ForSeek {
		panic("value type not valid")
	}
	return (uint64(seq) << 8) | uint64(t)
}

// UnpackSequenceAndType unpack tag to sequence number and value type
func UnpackSequenceAndType(tag uint64) (SequenceNumber, ValueType) {
	return SequenceNumber(tag >> 8), ValueType(tag & 0xff)
}

type ParsedInternalKey struct {
	UserKey  []byte
	Sequence SequenceNumber
	Type     ValueType
}

func NewParsedInternalKey(ukey []byte, seq SequenceNumber, t ValueType) *ParsedInternalKey {
	return &ParsedInternalKey{
		UserKey:  ukey,
		Sequence: seq,
		Type:     t,
	}
}

// DumpInternalKey dumps internal key to []byte
func DumpInternalKey(ikey *ParsedInternalKey) []byte {
	if ikey == nil {
		return nil
	}
	ret := make([]byte, len(ikey.UserKey)+TagSize)
	copy(ret, ikey.UserKey)
	copy(ret[len(ikey.UserKey):], util.EncodeUint64Fixed(PackSequenceAndType(ikey.Sequence, ikey.Type)))
	return ret
}

// ParseInternalKey parses bytes to ParsedInternalKey
func ParseInternalKey(ikey []byte) (*ParsedInternalKey, error) {
	n := len(ikey)
	if n < TagSize {
		return nil, Error(Code_InvalidArgument, "")
	}

	tagInt := util.DecodeUint64Fixed(ikey[n-TagSize:])
	seq, t := UnpackSequenceAndType(tagInt)
	if t > ValueType_Value {
		return nil, Error(Code_NotFound, "value type not valid")
	}
	return &ParsedInternalKey{
		UserKey:  ikey[:n-TagSize],
		Sequence: seq,
		Type:     t,
	}, nil
}
