package leveldb

type SequenceNumber uint64

// ValueType encoded as the last component of internal keys
type ValueType int

const (
	ValueType_Deletion ValueType = 0
	ValueType_Value    ValueType = 1
)

const ValueType_ForSeek = ValueType_Value
