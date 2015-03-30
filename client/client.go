package client

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
)

type Client struct {
	host     string
	port     int
	connPool *ConnPool
}

var (
	PUBLISH_FAIL = errors.New("publish fail")
	CONSUME_FAIL = errors.New("consume fail")

	HEARTBEAT_ERR = errors.New("heartbeat error")
)

func (client *Client) Publish(topic, msg []byte) error {

	conn, err := client.connPool.BorrowConn()
	defer client.connPool.ReturnConn(conn)

	if err != nil {
		return err
	}

	bb := commandBytes("publish", topic)

	_, err = conn.rw.Write(bb)

	if err != nil {
		return err
	}
	conn.rw.Flush()
	result, err := readResponse(conn.rw)
	if err != nil {
		return err
	}

	return nil
}

func (client *Client) Consume(topic, group []byte, maxSize int) ([][]byte, error) {

	conn, err := client.connPool.BorrowConn()
	defer client.connPool.ReturnConn(conn)

	bb := commandBytes("consume", topic, group, []byte(fmt.Sprintf("%d", maxSize)))

	_, err = conn.rw.Write(bb)

	if err != nil {
		return nil, err
	}
	conn.rw.Flush()

	result, err := readResponse(conn.rw)

	if err != nil {
		return nil, err
	}

	return result.([][]byte), nil
}

func commandBytes(cmd string, args ...[]byte) []byte {
	var buffer bytes.Buffer

	fmt.Fprintf(&buffer, "*%d\r\n$%d\r\n%s\r\n", len(args)+1, len(cmd), cmd)

	for _, arg := range args {
		fmt.Fprintf(&buffer, "$%d\r\n", len(arg))
		buffer.Write(arg)
		buffer.Write([]byte("\r\n"))
	}
	return buffer.Bytes()
}

func readResponse(rw *bufio.ReadWriter) (interface{}, error) {

	header, err := rw.ReadString('\n')

	if err != nil {
		return nil, err
	}

	header = string(trimRightCRLF([]byte(header)))

	if strings.HasPrefix(header, "+") {
		return header[1:], nil
	}

	if strings.HasPrefix(header, "-") {
		return nil, errors.New(header[1:])
	}

	if strings.HasPrefix(header, "$") {
		var length int
		fmt.Sscanf(header, "$%d", &length)
		bb := make([]byte, length)
		_, err := rw.Read(bb)
		if err != nil {
			return nil, err
		}
		return bb, nil
	}

	if strings.HasPrefix(header, "*") {
		var mLen int
		fmt.Sscanf(header, "*%d", &mLen)

		res := make([][]byte, mLen)

		for i := 0; i < mLen; i++ {
			lenStr, err := rw.ReadString('\n')
			if err != nil {
				return nil, err
			}
			var length int
			fmt.Sscanf(lenStr, "$%d", &length)
			bb := make([]byte, length)
			_, err = rw.Read(bb)
			if err != nil {
				return nil, err
			}
			res = append(res, bb)
		}

		return res, nil
	}
	return nil, errors.New("Error response.")

}

func trimRightCRLF(src []byte) []byte {
	if bytes.HasSuffix(src, []byte("\r\n")) {
		return bytes.TrimRight(src, "\r\n")
	}

	if bytes.HasSuffix(src, []byte("\n")) {
		return bytes.TrimRight(src, "\n")
	}

	return src

}

func SimpleqClient(host string, port, connSize int) (*Client, error) {

	pool := NewConnPool(host, port, connSize)

	return &Client{host: host, port: port, connPool: pool}, nil

}
