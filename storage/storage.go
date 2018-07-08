package storage

type Entry struct {
    Key []byte
    Val []byte
}

type Iterator interface {
    Next() (*Entry, error)
    Close() error
}

type Storage interface {
	Set(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)
    Close() error
    Seek(key []byte) Iterator
}
