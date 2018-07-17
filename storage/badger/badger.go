package badger

import (
	"anraft/storage"
	"github.com/dgraph-io/badger"
)

type BadgerIterator struct {
	iter *badger.Iterator
    txn *badger.Txn
}

func (i *BadgerIterator) Next() {
    i.iter.Next()
}

func (i *BadgerIterator) Entry() (*storage.Entry, error) {
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
    i.txn.Discard()
	return nil
}

func (i *BadgerIterator) ValidForPrefix(prefix []byte) bool {
    return i.iter.ValidForPrefix(prefix)
}

func (i *BadgerIterator) Valid() bool {
    return i.iter.Valid()
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
    defer txn.Discard()
    if err := txn.Set(key, val); err != nil {
        return err
    }
    return txn.Commit(nil)
}

func (b *BadgerEngine) Get(key []byte) ([]byte, error) {
	txn := b.db.NewTransaction(false)
    defer txn.Discard()
	itm, err := txn.Get(key)
	if err != nil {
        if err == badger.ErrKeyNotFound {
            return nil, storage.ErrKeyNotFound
        }
		return nil, err
	}
	return itm.Value()
}

func (b *BadgerEngine) Del(key []byte) error {
	txn := b.db.NewTransaction(true)
    defer txn.Discard()
    if err := txn.Delete(key); err != nil {
        return err
    }
    return txn.Commit(nil)
}

func (b *BadgerEngine) Seek(key []byte, forward bool) storage.Iterator {
	txn := b.db.NewTransaction(false)
    options := badger.IteratorOptions{
        PrefetchValues: true,
        PrefetchSize:   100,
        Reverse:        !forward,
        AllVersions:    false,
    }
	tmp := txn.NewIterator(options)
	tmp.Seek(key)
	return &BadgerIterator{
		iter: tmp,
        txn: txn,
	}
}
