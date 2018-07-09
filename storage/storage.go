package storage

type Entry struct {
    Key []byte
    Val []byte
}

type Iterator interface {
    Entry() (*Entry, error)
    Next()
    Close() error
    ValidForPrefix(prefix []byte) bool
    Valid() bool
}

type Storage interface {
	Set(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)
    Del(key []byte) error
    Close() error
    Seek(key []byte, forward bool) Iterator
}
