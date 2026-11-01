package kiroku

import (
	"context"
	"io"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gdbu/errors"
)

func TestNewProducer(t *testing.T) {
	type args struct {
		o                    Options
		src                  Source
		avoidDirectoryCreate bool
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
				o: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			wantErr: false,
		},
		{
			name: "basic with namespace",
			args: args{
				o: Options{
					Dir:       "./testing",
					Name:      "testing",
					Namespace: "scoped",
				},
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			wantErr: false,
		},
		{
			name: "invalid",
			args: args{
				o: MakeOptions("", ""),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				avoidDirectoryCreate: true,
			},
			wantErr: true,
		},
		{
			name: "missing directory",
			args: args{
				o: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				avoidDirectoryCreate: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.args.avoidDirectoryCreate {
				if err := os.Mkdir(tt.args.o.Dir, 0744); err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(tt.args.o.Dir)
			}

			p, err := NewProducer(tt.args.o, tt.args.src)
			if err == nil {
				defer p.Close()
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestProducer_BatchBlock(t *testing.T) {
	type fields struct {
		ctx  func() context.Context
		opts Options
		src  Source
	}

	type args struct {
		values [][]byte
	}

	type testcase struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foobar"),
				},
			},
			wantErr: false,
		},
		{
			name: "multiple",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foo"),
					[]byte("bar"),
					[]byte("baz"),
				},
			},
			wantErr: false,
		},
		{
			name: "cancelled context",
			fields: fields{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foobar"),
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

			tt.fields.opts.BatchDuration = time.Millisecond * 10
			p, err := NewProducerWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			for _, value := range tt.args.values {
				if err := p.BatchBlock(value); (err != nil) != tt.wantErr {
					t.Errorf("Producer.BatchBlock() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func TestProducer_Snapshot(t *testing.T) {
	type fields struct {
		ctx  func() context.Context
		opts Options
		src  Source
	}

	type args struct {
		values [][]byte
	}

	type testcase struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foobar"),
				},
			},
			wantErr: false,
		},
		{
			name: "multiple",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foo"),
					[]byte("bar"),
					[]byte("baz"),
				},
			},
			wantErr: false,
		},
		{
			name: "cancelled context",
			fields: fields{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				},
				opts: MakeOptions("./testing", "basic"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			args: args{
				values: [][]byte{
					[]byte("foobar"),
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

			p, err := NewProducerWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			if err := p.Snapshot(func(ss *Snapshot) (err error) {
				for _, value := range tt.args.values {
					if err = ss.Write(value); err != nil {
						return
					}
				}

				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Producer.Snapshot() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducer_Meta(t *testing.T) {
	type fields struct {
		opts       Options
		src        Source
		closeEarly bool
		onUpdate   func(Type, *Reader) error
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
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
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
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
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

			p, err := NewProducer(tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

			defer func() { _ = p.Close() }()

			if tt.fields.closeEarly {
				if err = p.Close(); err != nil {
					t.Fatal(err)
				}
			}

			gotMeta, err := p.Meta()
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

func TestProducer_Close(t *testing.T) {
	type fields struct {
		ctx  func() context.Context
		opts Options
		src  Source
	}

	type testcase struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
			},
			wantErr: false,
		},
		{
			name: "closed",
			fields: fields{
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
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

			p, err := NewProducerWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

			if err := p.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Producer.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducer_exportAndRemove(t *testing.T) {
	type fields struct {
		ctx         func() context.Context
		opts        Options
		src         Source
		filetype    Type
		avoidCreate bool
	}

	type testcase struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeChunk,
			},
			wantErr: false,
		},
		{
			name: "snapshot",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeSnapshot,
			},
			wantErr: false,
		},
		{
			name: "missing file",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype:    TypeSnapshot,
				avoidCreate: true,
			},
			wantErr: true,
		},
		{
			name: "error exporting chunk",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
						return filename, io.EOF
					},
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeChunk,
			},
			wantErr: true,
		},
		{
			name: "error exporting snapshot name",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func() func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
						var count int
						return func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
							count++
							if count < 2 {
								return filename, nil
							}
							return filename, io.EOF
						}
					}(),
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeSnapshot,
			},
			wantErr: true,
		},
		{
			name: "bad export filename",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
						return "foo." + filename, nil
					},
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeChunk,
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

			ctx, cancel := context.WithCancel(tt.fields.ctx())
			p, err := NewProducerWithContext(ctx, tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			// The producer's watcher exports and removes the chunks it finds in
			// its directory, so it is stopped before this test makes one and
			// calls exportAndRemove itself
			cancel()
			p.w.waitToComplete()

			fn := makeFilename(tt.fields.opts.FullName(), time.Now().UnixNano(), tt.fields.filetype)

			var w *Writer
			if !tt.fields.avoidCreate {
				if w, err = newWriter(tt.fields.opts.Dir, fn); err != nil {
					t.Fatal(err)
				}

				if err = w.Close(); err != nil {
					t.Fatal(err)
				}
			}

			if err = p.exportAndRemove(fn); (err != nil) != tt.wantErr {
				t.Errorf("Producer.exportAndRemove() error = %v, wantErr %v", err, tt.wantErr)
			}

			if err != nil {
				return
			}

			if _, err = os.Open(w.filepath); err == nil {
				t.Fatal("expected error for opening file, received nil")
			}
		})
	}
}

func TestProducer_transaction(t *testing.T) {
	type fields struct {
		ctx      func() context.Context
		opts     Options
		src      Source
		filetype Type

		avoidDirectoryCreate bool
		adjustFilename       bool
		errorCreatingWriter  bool
		returnError          bool
	}

	type testcase struct {
		name    string
		fields  fields
		wantErr bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype: TypeChunk,
			},
			wantErr: false,
		},
		{
			name: "missing file",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype:       TypeChunk,
				adjustFilename: true,
			},
			wantErr: true,
		},
		{
			name: "missing filepath",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype:    TypeChunk,
				returnError: true,
			},
			wantErr: true,
		},
		{
			name: "error creating writer",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
					func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
					func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
						return []string{}, nil
					},
					func(ctx context.Context, prefix, filename string) (Info, error) {
						return Info{}, io.EOF
					},
				),
				filetype:            TypeChunk,
				errorCreatingWriter: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !tt.fields.avoidDirectoryCreate {
				if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
					t.Fatal(err)
				}
				defer os.RemoveAll(tt.fields.opts.Dir)
			}

			if tt.fields.errorCreatingWriter {
				oldCreateAppend := createAppendFile
				defer func() { createAppendFile = oldCreateAppend }()
				createAppendFile = func(filename string) (*os.File, error) {
					return nil, io.EOF
				}
			}

			p, err := NewProducerWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()

			if err = p.transaction(TypeChunk, func(w *Writer) (err error) {
				if err = w.Write(Block("hello world")); err != nil {
					return
				}

				if tt.fields.adjustFilename {
					w.filename.Name = "EMPTY"
				}

				if tt.fields.returnError {
					return io.EOF
				}

				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Producer.transaction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProducer_TransactionWithoutSourceWritesNoChunk(t *testing.T) {
	dir := t.TempDir()
	p, err := NewProducer(MakeOptions(dir, "test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	// A producer without a Source has nowhere to export a chunk, so it must
	// not need to create one: its directory is made read-only
	if err = os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700)

	if err = p.Transaction(func(txn *Transaction) error {
		return txn.Write([]byte("hello world"))
	}); err != nil {
		t.Fatalf("a transaction without a source answered <%v>", err)
	}

	if err = p.Snapshot(func(ss *Snapshot) error {
		return ss.Write([]byte("hello world"))
	}); err != nil {
		t.Fatalf("a snapshot without a source answered <%v>", err)
	}
}

func TestProducer_SourcelessTransactionEncodesNothing(t *testing.T) {
	p, err := NewProducer(MakeOptions(t.TempDir(), "test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	type testcase struct {
		name string
		run  func(value []byte) error
	}

	tests := []testcase{
		{
			name: "transaction",
			run: func(value []byte) error {
				return p.Transaction(func(txn *Transaction) error { return txn.Write(value) })
			},
		},
		{
			name: "snapshot",
			run: func(value []byte) error {
				return p.Snapshot(func(ss *Snapshot) error { return ss.Write(value) })
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := []byte("hello world")
			// No Writer, enkodo Writer, Encoder or wrapper is made for a block
			// that is discarded (main allocated 6 times per run here)
			allocs := testing.AllocsPerRun(100, func() {
				if err := tt.run(value); err != nil {
					t.Fatal(err)
				}
			})
			if allocs != 0 {
				t.Fatalf("a %s without a source allocated %v times per run, want 0", tt.name, allocs)
			}

			if err := tt.run(nil); err != ErrEmptyBlock {
				t.Fatalf("an empty block answered <%v>, want <%v>", err, ErrEmptyBlock)
			}
		})
	}
}

func TestProducer_SourcelessStartsNoWatcher(t *testing.T) {
	p, err := NewProducer(MakeOptions(t.TempDir(), "test"), nil)
	if err != nil {
		t.Fatal(err)
	}

	if p.w != nil {
		t.Fatal("a producer without a source started a chunk watcher")
	}

	if err = p.Close(); err != nil {
		t.Fatalf("Close of a producer without a source answered <%v>", err)
	}

	if err = p.Close(); err != errors.ErrIsClosed {
		t.Fatalf("a second Close answered <%v>, want <%v>", err, errors.ErrIsClosed)
	}

	called := false
	if err = p.Transaction(func(*Transaction) error { called = true; return nil }); err != errors.ErrIsClosed || called {
		t.Fatalf("a Transaction after Close answered <%v> and ran its func: %v, want <%v> and false", err, called, errors.ErrIsClosed)
	}

	if err = p.Snapshot(func(*Snapshot) error { called = true; return nil }); err != errors.ErrIsClosed || called {
		t.Fatalf("a Snapshot after Close answered <%v> and ran its func: %v, want <%v> and false", err, called, errors.ErrIsClosed)
	}
}

func TestProducer_CloseReleasesMappedMeta(t *testing.T) {
	type fields struct {
		src                Source
		avoidExportOnClose bool
		cancelParent       bool
	}

	type testcase struct {
		name    string
		fields  fields
		wantErr error
	}

	src := newMockSource(
		func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) { return filename, nil },
		func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
		func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
		func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
		func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
			return []string{}, nil
		},
		func(ctx context.Context, prefix, filename string) (Info, error) {
			return Info{}, io.EOF
		},
	)

	tests := []testcase{
		{
			name: "without a source",
		},
		{
			name:   "with a source",
			fields: fields{src: src},
		},
		{
			name:   "with a source, avoiding export on close",
			fields: fields{src: src, avoidExportOnClose: true},
		},
		{
			name:    "without a source, after its parent context is cancelled",
			fields:  fields{cancelParent: true},
			wantErr: errors.ErrIsClosed,
		},
		{
			name:    "with a source, after its parent context is cancelled",
			fields:  fields{src: src, cancelParent: true},
			wantErr: errors.ErrIsClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := MakeOptions(t.TempDir(), "test")
			opts.AvoidExportOnClose = tt.fields.avoidExportOnClose
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			p, err := NewProducerWithContext(ctx, opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

			if tt.fields.cancelParent {
				cancel()
			}

			if err = p.Close(); err != tt.wantErr {
				t.Fatalf("Close answered <%v>, want <%v>", err, tt.wantErr)
			}

			if !p.m.closed || p.m.mm != nil {
				t.Fatal("Close left the meta mapped")
			}

			if _, err = p.m.f.Stat(); err == nil {
				t.Fatal("Close left the meta file's descriptor open")
			}
		})
	}
}

func TestProducer_CloseExportsBeforeReleasingMeta(t *testing.T) {
	var (
		exports  int32
		exported string
	)

	// The watcher's first export is held until Close has cancelled the
	// producer's context, then fails, so its retry sleep returns at once and
	// the chunk is exported by Close, whose meta must still be mapped to record
	// it. Every step waits on an event; none on elapsed time.
	attempted := make(chan struct{})
	release := make(chan struct{})
	src := newMockSource(
		func(ctx context.Context, prefix, filename string, r io.Reader) (string, error) {
			if atomic.AddInt32(&exports, 1) == 1 {
				close(attempted)
				<-release
				return "", io.ErrUnexpectedEOF
			}

			exported = filename
			return filename, nil
		},
		func(ctx context.Context, prefix, filename string, w io.Writer) error { return nil },
		func(ctx context.Context, prefix, filename string, fn func(io.Reader) error) error { return nil },
		func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
		func(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
			return []string{}, nil
		},
		func(ctx context.Context, prefix, filename string) (Info, error) {
			return Info{}, io.EOF
		},
	)

	opts := MakeOptions(t.TempDir(), "test")
	opts.OnError = func(error) {}
	p, err := NewProducer(opts, src)
	if err != nil {
		t.Fatal(err)
	}

	if err = p.Transaction(func(txn *Transaction) error {
		return txn.Write([]byte("hello world"))
	}); err != nil {
		t.Fatal(err)
	}

	<-attempted
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	<-p.ctx.Done()
	close(release)
	if err = <-closed; err != nil {
		t.Fatalf("Close answered <%v>", err)
	}

	want, err := makeMetaFromFilename(exported)
	if err != nil {
		t.Fatal(err)
	}

	m, err := newMappedMeta(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if got := m.Get(); got != want {
		t.Fatalf("the meta file holds <%+v> after Close exported a chunk, want <%+v>", got, want)
	}
}

// A Meta that races Close can find the meta released while the producer's
// context is still open: Close cancels, then releases. That state is made
// here directly, and Meta must answer ErrIsClosed rather than an empty Meta
// with no error.
func TestProducer_MetaAfterItsMetaIsReleased(t *testing.T) {
	p, err := NewProducer(MakeOptions(t.TempDir(), "test"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.cancelFn()

	if err = p.m.Close(); err != nil {
		t.Fatal(err)
	}

	if meta, err := p.Meta(); err != errors.ErrIsClosed || meta != (Meta{}) {
		t.Fatalf("Meta after its meta was released answered <%+v> <%v>, want an empty Meta and <%v>", meta, err, errors.ErrIsClosed)
	}
}
