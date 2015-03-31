package client

import (
	"sync"
	"testing"
)

func TestPublish(t *testing.T) {
	client, err := SimpleqClient("localhost", 9090, 1)

	if err != nil {
		t.Error(err)
	}

	err = client.Publish([]byte("topic1"), []byte("hi wens"))

	if err != nil {
		t.Error(err)
	}

}

func TestPublish2(t *testing.T) {

	n := 100000
	c := 100

	client, err := SimpleqClient("192.168.9.52", 9090, c)

	if err != nil {
		t.Error(err)
	}

	for i := 0; i < n; i++ {
		err = client.Publish([]byte("topic1"), []byte("hi wens"))
		if err != nil {
			t.Error(err)
		}
	}

	t.Log("Completed")

}

func TestPublish3(t *testing.T) {

	n := 100000
	c := 100

	client, err := SimpleqClient("192.168.9.52", 9090, c)

	ch := make(chan int, c)

	if err != nil {
		t.Error(err)
	}

	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		wg.Add(1)
		ch <- 1
		go func() {

			err = client.Publish([]byte("topic1"), []byte("hi wens"))
			<-ch
			wg.Done()
			if err != nil {
				t.Error(err)
			}
		}()

	}

	wg.Wait()

	t.Log("Completed")

}

func TestConsume(t *testing.T) {
	client, err := SimpleqClient("localhost", 9090, 1)

	if err != nil {
		t.Error(err)
	}

	msg, err := client.Consume([]byte("topic1"), []byte("g2"), 20)

	if err != nil {
		t.Error(err)
	}

	t.Log(len(msg))

}
