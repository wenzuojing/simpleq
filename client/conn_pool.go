package client

import (
	"time"
)

type ConnPool struct {
	poolSize int
	host     string
	port     int
	pool     chan *Conn
}

func (pool *ConnPool) BorrowConn() (*Conn, error) {

	conn := <-pool.pool

	if conn == nil {
		return OpenConn(pool.host, pool.port)
	}
	return conn, nil
}

func (pool *ConnPool) ReturnConn(conn *Conn) {
	select {
	case pool.pool <- conn:
	case <-time.After(time.Second):
		conn.conn.Close()
	}

}

func (pool *ConnPool) GetPoolSize() int {
	return pool.poolSize
}

func NewConnPool(host string, port, poolSize int) *ConnPool {

	p := make(chan *Conn, poolSize)

	for i := 0; i < poolSize; i++ {
		p <- nil
	}

	return &ConnPool{host: host, port: port, poolSize: poolSize, pool: p}
}
