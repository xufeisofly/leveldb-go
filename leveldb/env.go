package leveldb

type WritableFile interface {
	Append(data []byte) error
	Close() error
	Flush() error
	Sync() error
}
