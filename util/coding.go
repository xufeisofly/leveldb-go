package util

import (
	"encoding/binary"
	"unsafe"
)

// EncodeUvarint encodes uint64 to vary length byte array
func EncodeUvarint(v uint64) []byte {
	var l int
	tmp := v
	for tmp > 0 {
		tmp /= 0x80
		l++
	}
	if v == 0 {
		l = 1
	}
	bs := make([]byte, l)
	binary.PutUvarint(bs, v)
	return bs
}

// DecodeUvarint decodes byte array to varuint64
func DecodeUvarint(v []byte) (uint64, int) {
	r, num := binary.Uvarint(v)
	return r, num
}

// GetVarLengthPrefixedBytes gets data from |size(var) + data| structure
func GetVarLengthPrefixedBytes(bs []byte) ([]byte, uint64, int) {
	l, lsize := DecodeUvarint(bs)
	return bs[lsize : lsize+int(l)], l, lsize
}

func EncodeUint64Fixed(v uint64) []byte {
	bs := make([]byte, unsafe.Sizeof(v))
	binary.BigEndian.PutUint64(bs, v)
	return bs
}

func DecodeUint64Fixed(bs []byte) uint64 {
	return binary.BigEndian.Uint64(bs)
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
