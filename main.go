package main

import (
	"fmt"

	"github.com/xufeisofly/leveldb-go/util"
)

func main() {
	var str string
	util.AppendEscapedStringTo(&str, []byte{0, 0})
	fmt.Println(str)
}
