package goplumber

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// MemoryStore is a concurrency-safe in-memory k/v store.
type MemoryStore struct {
	kvStore map[string][]byte
	mux     sync.RWMutex
}

// NewMemoryStore returns a new instance of a MemoryStore
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{kvStore: map[string][]byte{}, mux: sync.RWMutex{}}
}

// Get allows MemoryStore to act as a DataSource.
func (ms *MemoryStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ms.mux.RLock()
	v, ok := ms.kvStore[key]
	ms.mux.RUnlock()
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}

// Put allows MemoryStore to act as a Sink.
func (ms *MemoryStore) Put(ctx context.Context, key string, value []byte) error {
	ms.mux.Lock()
	ms.kvStore[key] = value
	ms.mux.Unlock()
	return nil
}

// FileSystem loads files from a base directory.
type FileSystem struct {
	base string
}

// NewFileSystem returns a new FileSystem using the given base directory.
//
// The given base doesn't have to be absolute, but GetFile avoids "moving up"
// past the base directory path.
func NewFileSystem(base string) FileSystem {
	return FileSystem{base: base}
}

// getPath returns the 'name' appended to the base path, checked for a relative
// path that requires "moving up" a directory.
func (fs FileSystem) getPath(name string) (string, error) {
	path := filepath.FromSlash(name)
	path = filepath.Join(fs.base, path)
	relpath, err := filepath.Rel(fs.base, path)
	logrus.Debugf("request for '%s'", path)
	if err != nil {
		return "", errors.Wrapf(err, "unable to get path for '%s'", name)
	}
	if len(relpath) == 0 {
		return "", errors.Errorf("nothing to get - path was empty")
	}
	if len(relpath) < 2 || relpath[:2] == ".." {
		return "", errors.Errorf(
			"won't get '%s': paths must not move up directories", name)
	}
	return path, nil
}

// GetFile returns the byte data stored in a file of a given name.
func (fs FileSystem) GetFile(name string) ([]byte, error) {
	path, err := fs.getPath(name)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadFile(path)
}

// Get implements the DataSource interface for an FSLoader.
func (fs FileSystem) Get(ctx context.Context, key string) ([]byte, bool, error) {
	// Most ordinary files don't support read/write deadlines, so I'm not using
	// the context here. In general, there's no way to stop a goroutine blocked
	// on a file read.
	data, err := fs.GetFile(key)
	return data, !os.IsNotExist(err), err
}

// Put implements the Sink interface for an FSLoader.
//
// The file is created with 0666 (before masking), matching that used by Create.
func (fs FileSystem) Put(ctx context.Context, key string, value []byte) error {
	path, err := fs.getPath(key)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, value, 0666)
}
