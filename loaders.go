package goplumber

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"path/filepath"
	"sync"
)

// FSLoader loads files from a base directory.
type FSLoader struct {
	base string
}

func NewFSLoader(base string) FSLoader {
	return FSLoader{base: base}
}

func (fs FSLoader) GetFile(name string) ([]byte, error) {
	// Most ordinary files don't support read/write deadlines, so I'm not using
	// the context here. In general, there's no way to stop a blocked goroutine.
	path := filepath.FromSlash(name)
	path = filepath.Join(fs.base, path)
	relpath, err := filepath.Rel(fs.base, path)
	if err != nil || len(relpath) < 2 || relpath[:2] == ".." {
		return nil, errors.Errorf(
			"won't get '%s': paths must not move up directories", name)
	}
	return ioutil.ReadFile(path)
}

func (fs FSLoader) LoadTemplateNamespace(path string) (string, error) {
	if filepath.Ext(path) == "" {
		path += ".gotmpl"
	}
	body, err := fs.GetFile(path)
	return string(body), err
}

type DockerSecretsStore struct {
	FSLoader
}

func NewDockerSecretsStore(path string) DockerSecretsStore {
	return DockerSecretsStore{FSLoader: NewFSLoader(path)}
}
func (dss DockerSecretsStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	logrus.Debugf("fetching docker secret %s", key)
	data, err := dss.GetFile(key)
	return data, err == nil, err
}
func (dss DockerSecretsStore) Put(ctx context.Context, key string, value []byte) error {
	return errors.New("putting secrets in the DockerSecretsStore is prohibited")
}

// MemoryStore just keeps the k/v in memory.
type MemoryStore struct {
	kvStore map[string][]byte
	mux     sync.RWMutex
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{kvStore: map[string][]byte{}, mux: sync.RWMutex{}}
}

func (ms *MemoryStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ms.mux.RLock()
	v, ok := ms.kvStore[key]
	ms.mux.RUnlock()
	if !ok {
		return nil, false, nil
	}
	return v, true, nil
}

func (ms *MemoryStore) Put(ctx context.Context, key string, value []byte) error {
	ms.mux.Lock()
	ms.kvStore[key] = value
	ms.mux.Unlock()
	return nil
}

// todo: create a Consul Pipeline store & a mongo Pipeline store
