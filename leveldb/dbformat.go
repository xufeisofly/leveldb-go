package leveldb

type LookupKey struct {
	data   []byte
	start  uint64
	kstart uint64
	end    uint64
}

func NewLookupKey(userKey []byte, seq SequenceNumber) *LookupKey {
	return &LookupKey{}
}

func (lk *LookupKey) MemTableKey() []byte {
	return nil
}

func (lk *LookupKey) InternalKey() []byte {
	return nil
}

func (lk *LookupKey) UserKey() []byte {
	return nil
}
