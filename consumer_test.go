package kiroku

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hatchify/errors"
)

func TestNewConsumer(t *testing.T) {
	type args struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		avoidDirectory bool
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			args: args{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "avoid directory",
			args: args{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				avoidDirectory: true,
			},
			wantErr: true,
		},
		{
			name: "invalid opts",
			args: args{
				opts: MakeOptions("./testing", ""),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.args.avoidDirectory {
				if err := os.Mkdir(tt.args.opts.Dir, 0744); err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(tt.args.opts.Dir)
			}

			c, err := NewConsumer(tt.args.opts, tt.args.src, tt.args.onUpdate)
			if err == nil {
				defer func() { _ = c.Close() }()
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestNewOneShotConsumer(t *testing.T) {
	type args struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		avoidDirectory bool
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			args: args{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func() getFn {
						var count int
						return func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
							count++
							if count < 2 {
								return fn(strings.NewReader("test.12345.snapshot.kir"))
							}

							return fn(strings.NewReader("hello world"))
						}

					}(),
					func() getNextFn {
						var count int
						return func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
							count++
							if count < 2 {
								return "test.12345.chunk.kir", nil
							}

							return "", io.EOF
						}
					}(),
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "avoid directory",
			args: args{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				avoidDirectory: true,
			},
			wantErr: true,
		},
		{
			name: "error",
			args: args{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return errors.ErrIsClosed
					},
					func() getNextFn {
						var count int
						return func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
							count++
							if count < 2 {
								return "test.12345.chunk.kir", nil
							}

							return "", io.EOF
						}
					}(),
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.args.avoidDirectory {
				if err := os.Mkdir(tt.args.opts.Dir, 0744); err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(tt.args.opts.Dir)
			}

			err := NewOneShotConsumer(tt.args.opts, tt.args.src, tt.args.onUpdate)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_Meta(t *testing.T) {
	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error
	}

	type teststruct struct {
		name     string
		fields   fields
		wantMeta Meta
		wantErr  bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantMeta: Meta{
				LastProcessedTimestamp: 0,
				LastProcessedType:      TypeInvalid,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}

			defer func() { _ = c.Close() }()
			gotMeta, err := c.Meta()
			if (err != nil) != tt.wantErr {
				t.Errorf("Consumer.Meta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotMeta, tt.wantMeta) {
				t.Errorf("Consumer.Meta() = %v, want %v", gotMeta, tt.wantMeta)
			}
		})
	}
}

func TestConsumer_Close(t *testing.T) {
	type fields struct {
		opts       Options
		src        Source
		onUpdate   func(Type, *Reader) error
		closeEarly bool
	}

	type teststruct struct {
		name     string
		fields   fields
		wantMeta Meta
		wantErr  bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantMeta: Meta{
				LastProcessedTimestamp: 0,
				LastProcessedType:      TypeInvalid,
			},
			wantErr: false,
		},
		{
			name: "closed",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				closeEarly: true,
			},
			wantMeta: Meta{
				LastProcessedTimestamp: 0,
				LastProcessedType:      TypeInvalid,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}

			if tt.fields.closeEarly {
				c.close()
			}

			if err = c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.Close() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_sync(t *testing.T) {
	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error
	}

	type teststruct struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func() getNextFn {
						var count int
						return func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
							count++
							if count < 2 {
								fn := makeFilename("test", time.Now().UnixNano(), TypeChunk)
								return fn.String(), nil
							}

							return "", io.EOF
						}
					}(),
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if err = c.sync(); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.sync() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_getLatestSnapshot(t *testing.T) {
	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error
	}

	type teststruct struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return fn(strings.NewReader("text.12345.chunk.kir"))
					},
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "helloworld.txt", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "os.ErrNotExists",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return os.ErrNotExist },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return os.ErrNotExist
					},
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "EOF",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return io.EOF },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return fn(&mockReader{
							fn: func(bs []byte) (n int, err error) {
								return 0, io.EOF
							},
						})
					},
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "default error",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return io.EOF },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return fn(&mockReader{
							fn: func(bs []byte) (n int, err error) {
								return 0, errors.Error("no")
							},
						})
					},
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: true,
		},
		{
			name: "download error",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
						return filename, io.EOF
					},
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return io.EOF },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return fn(strings.NewReader("text.12345.snapshot.kir"))
					},
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "text.12346.chunk.kir", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if err = c.getLatestSnapshot(); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.getLatestSnapshot() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_shouldDownload(t *testing.T) {
	type args struct {
		latestSnapshot string
	}

	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		closeEarly bool
	}

	type teststruct struct {
		name   string
		args   args
		fields fields

		wantAfter bool
		wantErr   bool
	}

	tests := []teststruct{
		{
			name: "basic",
			args: args{
				latestSnapshot: "test.12345.snapshot.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantAfter: true,
			wantErr:   false,
		},
		{
			name: "closed",
			args: args{
				latestSnapshot: "test.12345.snapshot.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				closeEarly: true,
			},
			wantAfter: false,
			wantErr:   true,
		},
		{
			name: "empty latest",
			args: args{
				latestSnapshot: "",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				closeEarly: false,
			},
			wantAfter: false,
			wantErr:   false,
		},
		{
			name: "invalid latest",
			args: args{
				latestSnapshot: "test.snapshot.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				closeEarly: false,
			},
			wantAfter: false,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if tt.fields.closeEarly {
				_ = c.Close()
			}

			gotAfter, err := c.shouldDownload(tt.args.latestSnapshot)
			if (err != nil) != tt.wantErr {
				t.Errorf("Consumer.Meta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if gotAfter != tt.wantAfter {
				t.Errorf("Consumer.shouldDownload() = %v, want %v", gotAfter, tt.wantAfter)
			}
		})
	}
}

func TestConsumer_onChunk(t *testing.T) {
	type args struct {
		filename Filename
	}

	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		missingFile bool
	}

	type teststruct struct {
		name   string
		args   args
		fields fields

		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			args: args{
				filename: makeFilename("test", time.Now().UnixNano(), TypeChunk),
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
			},
			wantErr: false,
		},
		{
			name: "file doesn't exist",
			args: args{
				filename: makeFilename("test", time.Now().UnixNano(), TypeChunk),
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					return
				},
				missingFile: true,
			},
			wantErr: true,
		},
		{
			name: "cannot rename",
			args: args{
				filename: makeFilename("test", time.Now().UnixNano(), TypeChunk),
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) {
					f, ok := r.r.(*os.File)
					if !ok {
						err = fmt.Errorf("invalid type, expected <*os.File> and received <%T>", r.r)
						return
					}

					var stat os.FileInfo
					if stat, err = f.Stat(); err != nil {
						return
					}

					filepath := path.Join("./testing", stat.Name())
					newFilepath := path.Join("./testing", "hiding.txt")
					return renameFile(filepath, newFilepath)
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}

			if err = c.Close(); err != nil {
				t.Fatal(err)
			}

			if !tt.fields.missingFile {
				w, err := newWriter(tt.fields.opts.Dir, tt.args.filename)
				if err != nil {
					t.Errorf("Consumer.onChunk(): error initializing new writer: %v", err)
					return
				}

				if err = w.Write(Block("hello world!")); err != nil {
					t.Errorf("Consumer.onChunk(): error writing test file: %v", err)
					return
				}

				if err = w.Close(); err != nil {
					t.Errorf("Consumer.onChunk(): error closing writer for test file: %v", err)
					return
				}
			}

			if err = c.onChunk(tt.args.filename); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.onChunk() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_download(t *testing.T) {
	type args struct {
		filename string
	}

	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		cannotCreate bool
		cannotRename bool
	}

	type teststruct struct {
		name   string
		args   args
		fields fields

		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			args: args{
				filename: "test.12345.chunk.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: false,
		},
		{
			name: "import fail",
			args: args{
				filename: "test.12345.chunk.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error {
						return errors.Error("import fail")
					},
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
		{
			name: "cannot create",
			args: args{
				filename: "test.chunk.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate:     func(typ Type, r *Reader) (err error) { return },
				cannotCreate: true,
			},
			wantErr: true,
		},
		{
			name: "cannot rename",
			args: args{
				filename: "test.chunk.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate:     func(typ Type, r *Reader) (err error) { return },
				cannotRename: true,
			},
			wantErr: true,
		},
		{
			name: "invalid filename",
			args: args{
				filename: "test.chunk.kir",
			},
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			if tt.fields.cannotCreate {
				ofc := createFile
				defer func() { createFile = ofc }()
				createFile = func(filename string) (f *os.File, err error) {
					err = errors.Error("no")
					return
				}
			}

			if tt.fields.cannotRename {
				ofr := renameFile
				defer func() { renameFile = ofr }()

				renameFile = func(oldFilename, newFilename string) (err error) {
					err = errors.Error("no")
					return
				}
			}

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if err = c.download(tt.args.filename); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.onChunk() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_getNext(t *testing.T) {
	type fields struct {
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		closeEarly bool
	}

	type teststruct struct {
		name   string
		fields fields

		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "test.12345.chunk.kir", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: false,
		},
		{
			name: "close early",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "test.12345.chunk.kir", nil
					},
				),
				onUpdate:   func(typ Type, r *Reader) (err error) { return },
				closeEarly: true,
			},
			wantErr: true,
		},
		{
			name: "error getting next",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", errors.Error("nope")
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
		{
			name: "error downloading",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
						return filename, errors.Error("nope")
					},
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return errors.Error("nope") },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "test.12345.chunk.kir", nil
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(context.Background(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if tt.fields.closeEarly {
				c.Close()
			}

			if err = c.getNext(); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.onChunk() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestConsumer_scan(t *testing.T) {
	type fields struct {
		ctx      func() context.Context
		opts     Options
		src      Source
		onUpdate func(Type, *Reader) error

		closeEarly bool
	}

	type teststruct struct {
		name   string
		fields fields

		wantErr bool
	}

	tests := []teststruct{
		{
			name: "basic",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						time.Sleep(time.Millisecond * 10)
						cancel()
					}()
					return ctx
				},
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func() getNextFn {
						var count int
						return func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
							count++
							if count < 2 {
								return "test.12345.chunk.kir", nil
							}

							return "", errors.ErrIsClosed
						}
					}(),
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: false,
		},
		{
			name: "error downloading initial sync",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						time.Sleep(time.Millisecond * 10)
						cancel()
					}()
					return ctx
				},
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error {
						return errors.ErrIsClosed
					},
					func() getNextFn {
						var count int
						return func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
							count++
							if count < 2 {
								return "test.12345.chunk.kir", nil
							}

							return "", errors.ErrIsClosed
						}
					}(),
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
		{
			name: "EOF",
			fields: fields{
				opts: MakeOptions("./testing", "test"),
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					go func() {
						time.Sleep(time.Millisecond * 10)
						cancel()
					}()
					return ctx
				},
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", io.EOF
					},
				),
				onUpdate: func(typ Type, r *Reader) (err error) { return },
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			c, err := newConsumer(tt.fields.ctx(), tt.fields.opts, tt.fields.src, tt.fields.onUpdate)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close() }()

			if tt.fields.closeEarly {
				c.Close()
			}

			c.scan()
		})
	}
}
