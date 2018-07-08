package badger

import (
	"anraft/storage"
	"github.com/dgraph-io/badger"
)

type BadgerIterator struct {
	iter *badger.Iterator
}

func (i *BadgerIterator) Next() (*storage.Entry, error) {
	if i.iter.Valid() {
		return nil, nil
	}
	result := storage.Entry{}
	item := i.iter.Item()
	result.Key = item.Key()
	if tmp, err := item.Value(); err != nil {
		return nil, err
	} else {
		result.Val = tmp
		return &result, nil
	}
}

func (i *BadgerIterator) Close() error {
	i.iter.Close()
	return nil
}

type BadgerEngine struct {
	db *badger.DB
}

func (b *BadgerEngine) Init(dir string) error {
	// TODO(deyukong): background to do vlog gc
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}
	b.db = db
	return nil
}

func (b *BadgerEngine) Close() error {
	return b.db.Close()
}

func (b *BadgerEngine) Set(key []byte, val []byte) error {
	txn := b.db.NewTransaction(true)
	return txn.Set(key, val)
}

func (b *BadgerEngine) Get(key []byte) ([]byte, error) {
	txn := b.db.NewTransaction(false)
	itm, err := txn.Get(key)
	if err != nil {
		return nil, err
	}
	return itm.Value()
}

func (b *BadgerEngine) Seek(key []byte) storage.Iterator {
	txn := b.db.NewTransaction(false)
	tmp := txn.NewIterator(badger.DefaultIteratorOptions)
	tmp.Seek(key)
	return &BadgerIterator{
		iter: tmp,
	}
}
