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

// PutUvarint puts encoded varuint64 into buffer
func PutUvarint(buf *[]byte, v uint64) {
	*buf = append(*buf, EncodeUvarint(v)...)
}

// GetUvarint decodes byte array to varuint64 and puts the result into v
func GetUvarint(input *[]byte, v *uint64) bool {
	r, num := DecodeUvarint(*input)
	if num == 0 {
		return false
	}
	*input = (*input)[num:]
	*v = r
	return true
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

func EncodeUint32Fixed(v uint32) []byte {
	bs := make([]byte, unsafe.Sizeof(v))
	binary.BigEndian.PutUint32(bs, v)
	return bs
}

func DecodeUint64Fixed(bs []byte) uint64 {
	return binary.BigEndian.Uint64(bs)
}

func DecodeUint32Fixed(bs []byte) uint32 {
	return binary.BigEndian.Uint32(bs)
}

// PutUint64Fixed puts encoded fixed uint64 into buffer
func PutUint64Fixed(buf *[]byte, v uint64) {
	*buf = append(*buf, EncodeUint64Fixed(v)...)
}

// PutUint32Fixed puts encoded fixed uint32 into buffer
func PutUint32Fixed(buf *[]byte, v uint32) {
	*buf = append(*buf, EncodeUint32Fixed(v)...)
}

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Integer interface {
	Signed | Unsigned
}

func Min[T Integer](a, b T) T {
	if a < b {
		return a
	}
	return b
}
