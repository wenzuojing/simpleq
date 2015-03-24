package broker

import (
	"log"
	"strconv"
	"testing"
	"time"
)

func TestWrite(t *testing.T) {

	brokerStore := NewBrokerStore("/home/wens/data", "topic")

	start := time.Now()
	for i := 1; i <= 10001; i++ {
		err := brokerStore.Write([]byte("hello word !" + strconv.Itoa(i)))
		if err != nil {
			t.Errorf("Put fail : %v ", err)
		}
	}

	end := time.Now()

	t.Log(end.Sub(start).Seconds())

	err := brokerStore.Close()

	if err != nil {
		t.Errorf("Close fail : %v ", err)
	}

}

func TestRead(t *testing.T) {

	brokerStore := NewBrokerStore("/home/wens/data", "t1")

	start := time.Now()

	for {

		bb, err := brokerStore.Read([]byte("my2"), 10)

		if err != nil {
			t.Errorf("Get fail : %v ", err)
		}

		if len(bb) == 0 {
			log.Println("not data")
			time.Sleep(time.Second)

		} else {
			for _, d := range bb {
				log.Println(string(d))
			}
		}

	}

	end := time.Now()

	t.Log(end.Sub(start).Seconds())

	err := brokerStore.Close()

	if err != nil {
		t.Errorf("Close fail : %v ", err)
	}
}
