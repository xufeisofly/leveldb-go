package leveldb

type Comparator interface {
	Compare(a, b []byte) int8
}
