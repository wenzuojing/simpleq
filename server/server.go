package server

import (
	"bufio"
	"fmt"
	simple "github.com/wenzuojing/simpleq/broker"
	"io"
	"log"
	"net"
	"runtime"
	"strconv"
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

		cmd, _, err := rw.ReadLine()

		if err != nil {
			panic(err)
		}

		if len(cmd) == 0 {
			continue
		}

		switch string(cmd) {
		case "publish":
			err := handlePublish(rw)

			if err != nil {
				panic(err)
			}
			break
		case "consume":
			err := handleConsume(rw)
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

func writeBytes(writer io.Writer, data []byte) error {
	_, err := writer.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func readBytes(reader io.Reader, len int) ([]byte, error) {
	buf := make([]byte, len)
	_, err := reader.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func handlePublish(rw *bufio.ReadWriter) error {

	topic, _, err := rw.ReadLine()

	if err != nil {
		return err
	}

	mLen, _, err := rw.ReadLine()
	if err != nil {
		return err
	}

	msgLen, err := strconv.Atoi(string(mLen))
	msg, err := readBytes(rw, msgLen)

	if err != nil {
		return err
	}

	err = broker.Write([]byte(topic), msg)

	if err != nil {
		return err
	}

	if err := writeBytes(rw, []byte("ok\n")); err != nil {
		return err
	}

	rw.Flush()
	return nil

}

func handleConsume(rw *bufio.ReadWriter) error {

	topic, _, err := rw.ReadLine()

	if err != nil {
		return err
	}
	group, _, err := rw.ReadLine()

	if err != nil {
		return err
	}

	msgs, err := broker.Read(topic, group, 1)

	if len(msgs) == 0 {
		if err := writeBytes(rw, []byte("nil\n")); err != nil {
			return err
		}

	} else {
		if err := writeBytes(rw, []byte("ok\n")); err != nil {
			return err
		}

		if err := writeBytes(rw, []byte(strconv.Itoa(len(msgs[0]))+"\n")); err != nil {
			return err
		}

		if err := writeBytes(rw, msgs[0]); err != nil {
			return err
		}

	}
	rw.Flush()
	return nil

}
