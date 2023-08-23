package kiroku

import (
	"bytes"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/hatchify/errors"
	"github.com/mojura/enkodo"
)

func TestRead(t *testing.T) {
	type args struct {
		filename string
	}

	type testcase struct {
		name     string
		prep     func() error
		teardown func()
		args     args
		wantErr  bool
	}

	tests := []testcase{
		{
			name: "basic",
			prep: func() (err error) {
				_, err = createFile("test.txt")
				return
			},
			teardown: func() {
				os.Remove("test.txt")
			},
			args: args{
				filename: "./test.txt",
			},
			wantErr: false,
		},
		{
			name: "file doesn't exist",
			prep: func() (err error) {
				return
			},
			teardown: func() {},
			args: args{
				filename: "./test.txt",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.prep(); err != nil {
				t.Fatal(err)
			}
			defer tt.teardown()

			if err := Read(tt.args.filename, func(r *Reader) (err error) {
				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReader_ForEach(t *testing.T) {
	type fields struct {
		getReader func() (File, error)
	}

	type args struct {
		seek int64
		fn   func(Block) error
	}

	type testcase struct {
		name     string
		fields   fields
		args     args
		teardown func()
		wantErr  bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				getReader: func() (r File, err error) {
					buf := bytes.NewBuffer(nil)
					if err = enkodo.NewWriter(buf).Encode(Block("hello world")); err != nil {
						return
					}

					r = bytes.NewReader(buf.Bytes())
					return
				},
			},
			args: args{
				fn: func(b Block) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "with error",
			fields: fields{
				getReader: func() (r File, err error) {
					buf := bytes.NewBuffer(nil)
					w := enkodo.NewWriter(buf)
					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					r = bytes.NewReader(buf.Bytes())
					return
				},
			},
			args: args{
				fn: func(b Block) (err error) {
					return errors.ErrIsClosed
				},
			},
			wantErr: true,
		},
		{
			name: "seek error",
			fields: fields{
				getReader: func() (r File, err error) {
					buf := bytes.NewBuffer(nil)
					w := enkodo.NewWriter(buf)
					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					var f *os.File
					if f, err = createFile("./test.txt"); err != nil {
						return
					}

					f.Close()
					r = f
					return
				},
			},
			teardown: func() {
				os.Remove("./test.txt")
			},
			args: args{
				fn: func(b Block) (err error) {
					return errors.ErrIsClosed
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdr, err := tt.fields.getReader()
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				if tt.teardown != nil {
					tt.teardown()
				}
			}()

			r := NewReader(rdr)
			err = r.ForEach(tt.args.seek, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("Reader.ForEach() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReader_Copy(t *testing.T) {
	type fields struct {
		getReader func() (File, error)
	}

	type args struct {
		fn func(Block) error
	}

	type testcase struct {
		name     string
		fields   fields
		args     args
		teardown func()
		wantErr  bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				getReader: func() (r File, err error) {
					buf := bytes.NewBuffer(nil)
					if err = enkodo.NewWriter(buf).Encode(Block("hello world")); err != nil {
						return
					}

					r = bytes.NewReader(buf.Bytes())
					return
				},
			},
			args: args{
				fn: func(b Block) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "seek error",
			fields: fields{
				getReader: func() (r File, err error) {
					buf := bytes.NewBuffer(nil)
					w := enkodo.NewWriter(buf)
					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					if err = w.Encode(Block("hello world")); err != nil {
						return
					}

					var f *os.File
					if f, err = createFile("./test.txt"); err != nil {
						return
					}

					f.Close()
					r = f
					return
				},
			},
			teardown: func() {
				os.Remove("./test.txt")
			},
			args: args{
				fn: func(b Block) (err error) {
					return errors.ErrIsClosed
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdr, err := tt.fields.getReader()
			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				if tt.teardown != nil {
					tt.teardown()
				}
			}()

			r := NewReader(rdr)

			got := bytes.NewBuffer(nil)
			if _, err := r.Copy(got); (err != nil) != tt.wantErr {
				t.Errorf("Reader.ForEach() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReader_ReadSeeker(t *testing.T) {
	type fields struct {
		r io.ReadSeeker
	}

	type testcase struct {
		name   string
		fields fields
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				r: bytes.NewReader(nil),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{
				r: tt.fields.r,
			}
			if got := r.ReadSeeker(); !reflect.DeepEqual(got, tt.fields.r) {
				t.Errorf("Reader.ReadSeeker() = %v, want %v", got, tt.fields.r)
			}
		})
	}
}

func TestReader_handleError(t *testing.T) {
	type args struct {
		inbound error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name:    "nil error",
			args:    args{inbound: nil},
			wantErr: false,
		},
		{
			name:    "EOF error",
			args:    args{inbound: io.EOF},
			wantErr: false,
		},
		{
			name:    "default error",
			args:    args{inbound: errors.New("this is a test")},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Reader{}
			if err := r.handleError(tt.args.inbound); (err != nil) != tt.wantErr {
				t.Errorf("Reader.handleError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
