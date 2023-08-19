package kiroku

import "io"

var _ io.Reader = &mockReader{}

type mockReader struct {
	fn func(bs []byte) (n int, err error)
}

func (m *mockReader) Read(bs []byte) (n int, err error) {
	return m.fn(bs)
}
