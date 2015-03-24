package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
)

type Client struct {
	host string
	port int
	conn *net.TCPConn
	rw   *bufio.ReadWriter
	mu   sync.Mutex
}

var (
	PUBLISH_FAIL = errors.New("publish fail")
	CONSUME_FAIL = errors.New("consume fail")
	ERR_ACK      = errors.New("error ack")
)

func (client *Client) Publish(topic, msg []byte) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	topic = append(topic, '\n')
	client.rw.Write([]byte("publish\n"))
	client.rw.Write(topic)

	client.rw.Write([]byte(strconv.Itoa(len(msg)) + "\n"))
	client.rw.Write([]byte(msg))
	client.rw.Flush()

	ack, _, err := client.rw.ReadLine()

	if err != nil {
		return nil
	}

	if "ok" == string(ack) {
		return nil
	}

	return PUBLISH_FAIL
}

func (client *Client) Consume(topic, group []byte) ([]byte, error) {

	client.mu.Lock()
	defer client.mu.Unlock()

	topic = append(topic, '\n')
	group = append(group, '\n')
	client.rw.Write([]byte("consume\n"))
	client.rw.Write(topic)
	client.rw.Write(group)
	client.rw.Flush()

	ack, _, err := client.rw.ReadLine()

	if err != nil {
		return nil, err
	}

	if "ok" == string(ack) {
		mLen, _, err := client.rw.ReadLine()
		if err != nil {
			return nil, err
		}
		msgLen, _ := strconv.Atoi(string(mLen))
		buf := make([]byte, msgLen)
		_, err = client.rw.Read(buf)
		if err != nil {
			return nil, err
		}
		return buf, nil
	} else if "nil" == string(ack) {
		return nil, nil
	}
	return nil, CONSUME_FAIL
}

func (client *Client) Heartbeat() error {

	client.mu.Lock()
	defer client.mu.Unlock()

	_, err := client.rw.Write([]byte("heartbeat\n"))

	if err != nil {
		return err
	}

	ack, _, err := client.rw.ReadLine()

	if err != nil {
		return nil
	}

	if "ok" == string(ack) {
		return nil
	}

	return ERR_ACK
}

func (client *Client) Close() error {
	return client.conn.Close()
}

func NewClient(host string, port int) (*Client, error) {

	addr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}

	listener, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return nil, err
	}
	return &Client{conn: listener, rw: bufio.NewReadWriter(bufio.NewReader(listener), bufio.NewWriter(listener))}, nil

}
