package util

import (
	"encoding/binary"
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
func DecodeUvarint(v []byte) uint64 {
	r, _ := binary.Uvarint(v)
	return r
}
