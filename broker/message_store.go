package broker

import (
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"log"
	"sync"
)

var MAX_SIZE uint64 = 1000000
var FULL_SIZE_ERROR = errors.New("store full size")
var STORE_CLOSED_ERROR = errors.New("store closed")
var EOF_ERROR = errors.New("read store end")

type MessageStore struct {
	mu   sync.Mutex
	meta *Meta
	db   *leveldb.DB
}

func (store *MessageStore) Write(data []byte) error {

	store.mu.Lock()
	defer store.mu.Unlock()

	size, err := store.meta.Size()

	if err != nil {
		return err
	}

	if size >= MAX_SIZE {

		return FULL_SIZE_ERROR
	}

	key := fmt.Sprintf("%020d", size)

	err = store.db.Put([]byte(key), data, nil)

	if err != nil {
		if err == leveldb.ErrClosed {
			return STORE_CLOSED_ERROR
		}
		return err
	}

	return store.meta.SetSize(size + 1)
}

func (store *MessageStore) Read(group []byte, size int) ([][]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	position, err := store.meta.GetReadPosition(group)

	if err != nil {
		return nil, err
	}

	if position >= MAX_SIZE {
		return nil, EOF_ERROR
	}

	start := fmt.Sprintf("%020d", position)
	limit := fmt.Sprintf("%020d", position+uint64(size))

	it := store.db.NewIterator(&util.Range{Start: []byte(start), Limit: []byte(limit)}, nil)
	defer it.Release()

	result := make([][]byte, 0, size)
	for it.Next() {
		value := make([]byte, len(it.Value()))
		copy(value, it.Value())
		result = append(result, value)
	}
	newPosition := position + uint64(len(result))
	log.Printf("%s pull from %d to %d", group, position, newPosition)
	err = store.meta.SetReadPosition(group, newPosition)
	if err != nil {
		return nil, err
	}
	return result, nil

}

func (store *MessageStore) Close() error {
	return store.db.Close()
}
