package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"
)

var ACK_ERR = errors.New("ack error")

type Conn struct {
	conn *net.TCPConn
	rw   *bufio.ReadWriter
}

func OpenConn(host string, port int) (*Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", host, port))

	if err != nil {
		return nil, err
	}
	listener, err := net.DialTCP("tcp", nil, addr)

	if err != nil {
		return nil, err
	}
	return &Conn{conn: listener, rw: bufio.NewReadWriter(bufio.NewReader(listener), bufio.NewWriter(listener))}, nil
}
