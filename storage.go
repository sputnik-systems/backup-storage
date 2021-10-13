package storage

import (
	"io"
	"time"
)

type Storage interface {
	List() ([]FileInfo, error)
	Delete(string) error
	Upload(string, io.Reader) error
	Download(string, io.Writer) error
}

type FileInfo interface {
	Name() string
	Size() int64
	ModTime() time.Time
	IsDir() bool
}
