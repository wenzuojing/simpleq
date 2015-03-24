package broker

type Store interface {
	Write(data []byte) error
	Read(group []byte, size int) ([][]byte, error)
	Close() error
}
