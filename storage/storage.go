package storage

type Storage interface {
	Set(key string, val []byte) error
	Get(key string) ([]byte, error)
}
