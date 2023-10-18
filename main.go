package main

import (
	"fmt"

	"github.com/xufeisofly/leveldb-go/util"
)

func main() {
	num := uint64(36824)
	bs := util.EncodeUvarint(num)
	fmt.Println(append(bs, util.EncodeUvarint(999)...))
	fmt.Println(util.DecodeUvarint(bs))
}
