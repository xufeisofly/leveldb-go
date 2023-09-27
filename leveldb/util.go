package leveldb

import (
	"github.com/xufeisofly/leveldb-go/util"
)

func Uint64ToByteArr(a uint64) []byte {
	return util.EncodeUvarint(a)
	// return []byte(cast.ToString(a))
	// bs := make([]byte, 8)
	// binary.LittleEndian.PutUint64(bs, a)
	// return bs
}

// func ByteArrToUint64(arr []byte) uint64 {
// 	return binary.BigEndian.Uint64(arr)
// }
