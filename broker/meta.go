package broker

import (
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
	"path"
	"sync"
)

type Meta struct {
	mu sync.Mutex
	db *leveldb.DB
}

func (meta *Meta) Size() (uint64, error) {

	value, err := meta.db.Get([]byte("size"), nil)

	if err != nil {
		if leveldb.ErrNotFound == err {
			return 0, nil
		}
		return 0, nil
	}

	return binary.BigEndian.Uint64(value), nil
}

func (meta *Meta) SetSize(size uint64) error {
	bb := make([]byte, 8)
	binary.BigEndian.PutUint64(bb, size)
	return meta.db.Put([]byte("size"), bb, nil)
}

func (meta *Meta) GetReadPosition(group []byte) (uint64, error) {

	position, err := meta.db.Get(group, nil)
	if err != nil {
		if leveldb.ErrNotFound == err {
			return uint64(0), nil
		}
		return 0, err
	}

	return binary.BigEndian.Uint64(position), nil
}

func (meta *Meta) SetReadPosition(group []byte, position uint64) error {
	bb := make([]byte, 8)
	binary.BigEndian.PutUint64(bb, position)
	return meta.db.Put(group, bb, nil)
}

func (meta *Meta) GetWriteStoreSeq() (uint32, error) {
	seq, err := meta.db.Get([]byte("write_seq"), nil)
	if err != nil {
		if leveldb.ErrNotFound == err {
			err := meta.SetWriteStoreSeq(uint32(0))

			if err != nil {
				return uint32(0), err
			}

			return uint32(0), nil
		}
		return 0, err
	}

	return binary.BigEndian.Uint32(seq), nil
}

func (meta *Meta) SetWriteStoreSeq(seq uint32) error {
	bb := make([]byte, 4)
	binary.BigEndian.PutUint32(bb, seq)
	return meta.db.Put([]byte("write_seq"), bb, nil)
}

func (meta *Meta) GetReadStoreSeq(group []byte) (uint32, error) {
	seq, err := meta.db.Get([]byte(string(group)+"read_seq"), nil)
	if err != nil {
		if leveldb.ErrNotFound == err {
			return uint32(0), nil
		}
		return 0, err
	}

	return binary.BigEndian.Uint32(seq), nil
}

func (meta *Meta) SetReadStoreSeq(group []byte, seq uint32) error {
	bb := make([]byte, 4)
	binary.BigEndian.PutUint32(bb, seq)
	return meta.db.Put([]byte(string(group)+"read_seq"), bb, nil)
}

func (meta Meta) Close() {
	meta.db.Close()
}

func NewMeta(dataDir string) *Meta {
	db, err := leveldb.OpenFile(path.Join(dataDir, "_meta"), nil)
	if err != nil {
		panic(err)
	}
	return &Meta{db: db}

}
