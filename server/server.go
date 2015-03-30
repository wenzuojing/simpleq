package server

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	simple "github.com/wenzuojing/simpleq/broker"
	"io"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var broker = simple.NewBroker("/home/wens/data")

func StartServer(host string, port int) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	addr, err := net.ResolveTCPAddr("tcp4", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				log.Fatalf("[error]%v", err)
			}
			go handleConn(conn)
		}
	}()
	return nil
}

func handleConn(conn *net.TCPConn) {
	defer func() {
		if x := recover(); x != nil {
			log.Printf("[error]%v\n", x)
		}
	}()
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	for {
		conn.SetReadDeadline(time.Now().Add(time.Minute * 10))

		cmd, args, err := parseRequest(rw)

		if err != nil {
			panic(err)
		}

		switch string(cmd) {

		case "publish":
			err := handlePublish(args, rw)

			if err != nil {
				panic(err)
			}
			break
		case "consume":
			err := handleConsume(args, rw)
			if err != nil {
				panic(err)
			}
			break
		case "heartbeat":
			if err := writeBytes(rw, []byte("ok\n")); err != nil {
				panic(err)
			}

			rw.Flush()

		default:
			panic("unknow cmd :" + string(cmd))
		}

	}

}

func parseRequest(rw *bufio.ReadWriter) (cmd []byte, args [][]byte, err error) {

	header, err := rw.ReadString('\n')

	if err != nil {
		return
	}

	header = string(trimRightCRLF([]byte(header)))

	if strings.HasPrefix(header, "*") {
		var mLen int
		fmt.Sscanf(header, "*%d", &mLen)

		args = make([][]byte, mLen)

		for i := 0; i < mLen; i++ {
			lenStr, err := rw.ReadString('\n')
			if err != nil {
				return nil, nil, err
			}
			var length int
			fmt.Sscanf(lenStr, "$%d", &length)
			bb := make([]byte, length)
			_, err = rw.Read(bb)
			if err != nil {
				return nil, nil, err
			}
			args = append(args, bb)
		}

		cmd = args[0]
		args = args[1:]
		return
	} else {
		err = errors.New("Error protocol")
		return
	}

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

func writeBytes(writer io.Writer, data []byte) error {
	_, err := writer.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func handlePublish(args [][]byte, rw *bufio.ReadWriter) error {

	if len(args) != 2 {
		return errors.New("Bad parameter.")
	}

	topic := args[0]
	msg := args[1]

	err := broker.Write([]byte(topic), msg)

	if err != nil {
		return err
	}

	_, err = rw.Write([]byte("+ok\r\n"))

	if err != nil {
		return err
	}

	rw.Flush()
	return nil

}

func handleConsume(args [][]byte, rw *bufio.ReadWriter) error {

	if len(args) != 3 {
		return errors.New("Bad parameter.")
	}

	topic := args[0]
	group := args[1]
	size, _ := strconv.Atoi(string(args[2]))

	msgs, err := broker.Read(topic, group, size)
	if err != nil {
		return err
	}

	var buffer bytes.Buffer

	buffer.Write([]byte(fmt.Sprintf("*%d\r\n", len(msgs))))

	for _, msg := range msgs {

		buffer.Write([]byte(fmt.Sprintf("$%d\r\n", len(msg))))
		buffer.Write(msg)

	}
	_, err = rw.Write(buffer.Bytes())

	if err != nil {
		return err
	}
	rw.Flush()
	return nil

}
