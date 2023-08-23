package kiroku

import (
	"io"
	"io/fs"
	"time"
)

var _ io.Reader = &mockReader{}

type mockReader struct {
	fn func(bs []byte) (n int, err error)
}

func (m *mockReader) Read(bs []byte) (n int, err error) {
	return m.fn(bs)
}

type mockFileInfo struct {
	isDir bool
}

func (m *mockFileInfo) Name() string {
	return "test"
}

func (m *mockFileInfo) Size() int64 {
	return 1337
}

func (m *mockFileInfo) Mode() fs.FileMode {
	return 0
}

func (m *mockFileInfo) ModTime() time.Time {
	return time.Now()
}

func (m *mockFileInfo) IsDir() bool {
	return m.isDir
}

func (m *mockFileInfo) Sys() any {
	return nil
}
