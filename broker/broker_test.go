package broker

import (
	"testing"
)

func TestBrokerWrite(t *testing.T) {
	b := NewBroker("/home/wens/data")

	if err := b.Write([]byte("topic2"), []byte("hi wens!")); err != nil {
		t.Error("err:", err)
	}

}

func TestBrokerRead(t *testing.T) {
	b := NewBroker("/home/wens/data")

	data, _ := b.Read([]byte("t2"), []byte("my"), 10)

	for _, m := range data {
		t.Log(string(m))
	}

}
