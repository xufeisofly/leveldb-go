package leveldb

type Code int

type LevelError struct {
	Code Code
	Msg  string
}

var _ error = (*LevelError)(nil)

func (e *LevelError) Error() string {
	return e.Msg
}

const (
	Code_NotFound Code = iota + 1
	Code_Corruption
	Code_NotSupported
	Code_InvalidArgument
	Code_IOError
)

func Error(code Code, msg string) error {
	return &LevelError{Code: code, Msg: msg}
}

func (e *LevelError) IsNotFound() bool {
	return e.Code == Code_NotFound
}

func (e *LevelError) IsCorruption() bool {
	return e.Code == Code_Corruption
}

func (e *LevelError) IsNotSupported() bool {
	return e.Code == Code_NotSupported
}

func (e *LevelError) IsInvalidArgument() bool {
	return e.Code == Code_InvalidArgument
}

func (e *LevelError) IsIOError() bool {
	return e.Code == Code_IOError
}
