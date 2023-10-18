package leveldb

type SequenceNumber uint64

// ValueType encoded as the last component of internal keys
type ValueType int

const (
	ValueType_Deletion ValueType = 0
	ValueType_Value    ValueType = 1
)

const ValueType_ForSeek = ValueType_Value

type CompressionType int

const (
	CompressionType_NoCompression CompressionType = 0x0
	CompressionType_Snappy        CompressionType = 0x1
	CompressionType_Zstd          CompressionType = 0x2
)

const (
	Uint64Size = 8
	Uint32Size = 4
)
