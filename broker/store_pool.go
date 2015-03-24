package broker

import (
	"github.com/syndtr/goleveldb/leveldb"
	"log"
	"sync"
	"time"
)

type WrapStore struct {
	store    Store
	lastUsed time.Time
}

func (wq *WrapStore) Write(data []byte) error {
	wq.lastUsed = time.Now()
	return wq.store.Write(data)
}

func (wq *WrapStore) Read(group []byte, size int) ([][]byte, error) {
	wq.lastUsed = time.Now()
	return wq.store.Read(group, size)
}

func (wq *WrapStore) Close() error {
	wq.lastUsed = time.Now()
	return wq.store.Close()
}

var openedStores = make(map[string]*WrapStore)

var mu sync.Mutex

func init() {

	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			<-ticker.C
			log.Println("bengin check unuse store ")
			if err := closeUnuseStore(); err != nil {
				log.Println("check unuse store fail . ", err)
			}
			log.Println("complete check unuse store ")

		}
	}()
}

func closeUnuseStore() error {
	mu.Lock()
	defer mu.Unlock()

	removed := make([]string, 0)
	for d, q := range openedStores {
		if time.Now().Sub(q.lastUsed).Minutes() > 30 {
			if err := q.Close(); err != nil {
				return err
			}
			removed = append(removed, d)
			log.Printf("close store :%s\n", d)

		}
	}

	for _, d := range removed {
		delete(openedStores, d)
	}

	return nil

}

func GetStore(dataDir string, meta *Meta) *WrapStore {
	mu.Lock()
	defer mu.Unlock()
	if store, ok := openedStores[dataDir]; ok {
		return store
	}
	db, err := leveldb.OpenFile(dataDir, nil)

	if err != nil {
		panic(err)
	}
	messageStore := &MessageStore{meta: meta, db: db}
	store := &WrapStore{store: messageStore}
	openedStores[dataDir] = store
	return store
}
