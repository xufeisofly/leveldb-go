package util

import "fmt"

func EscapeString(v []byte) string {
	var str string
	AppendEscapedStringTo(&str, v)
	return str
}

func AppendEscapedStringTo(str *string, v []byte) {
	for _, b := range v {
		if b >= ' ' && b <= '~' {
			*str += string(b)
		} else {
			s := fmt.Sprintf("\\x%02x", b&0xff)
			*str += s
		}
	}
}
