package client

import (
	"testing"
)

func TestPublish(t *testing.T) {
	client, err := NewClient("localhost", 9090)

	if err != nil {
		t.Error(err)
	}

	err = client.Publish([]byte("topic1"), []byte("hi wens"))

	if err != nil {
		t.Error(err)
	}

	client.Close()

}

func TestConsume(t *testing.T) {
	client, err := NewClient("localhost", 9090)

	if err != nil {
		t.Error(err)
	}

	msg, err := client.Consume([]byte("topic1"), []byte("g2"))

	if err != nil {
		t.Error(err)
	}

	t.Log(string(msg))

	client.Close()

}
