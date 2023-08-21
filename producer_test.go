package kiroku

import (
	"context"
	"io"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	type args struct {
		o   Options
		src Source
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "basic",
			args: args{
				o: MakeOptions("./", "test"),
				src: newMockSource(
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
			},
			wantErr: false,
		},
		{
			name: "basic with namespace",
			args: args{
				o: Options{
					Dir:       "./",
					Name:      "testing",
					Namespace: "scoped",
				},
				src: newMockSource(
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
			},
			wantErr: false,
		},
		{
			name: "invalid",
			args: args{
				o: MakeOptions("", ""),
				src: newMockSource(
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.args.o, tt.args.src)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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

			p, err := NewWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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

			p, err := NewWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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

			p, err := NewWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				filetype: TypeChunk,
			},
			wantErr: false,
		},
		{
			name: "no source",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts:     MakeOptions("./testing", "test"),
				src:      nil,
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return io.EOF },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func() func(ctx context.Context, filename string, r io.Reader) error {
						var count int
						return func(ctx context.Context, filename string, r io.Reader) error {
							count++
							if count < 2 {
								return nil
							}
							return io.EOF
						}
					}(),
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				filetype: TypeSnapshot,
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

			p, err := NewWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

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
		ctx                  func() context.Context
		opts                 Options
		src                  Source
		filetype             Type
		avoidDirectoryCreate bool
		adjustFilename       bool
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				filetype: TypeChunk,
			},
			wantErr: false,
		},
		{
			name: "missing directory",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				filetype:             TypeChunk,
				avoidDirectoryCreate: true,
			},
			wantErr: true,
		},
		{
			name: "missing file",
			fields: fields{
				ctx: func() context.Context {
					return context.Background()
				},
				opts: MakeOptions("./testing", "test"),
				src: newMockSource(
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				filetype:    TypeChunk,
				returnError: true,
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

			p, err := NewWithContext(tt.fields.ctx(), tt.fields.opts, tt.fields.src)
			if err != nil {
				t.Fatal(err)
			}

			if err = p.transaction(TypeChunk, func(w *Writer) (err error) {
				if err = w.Write(Block("hello world")); err != nil {
					return
				}

				if tt.fields.adjustFilename {
					w.filename.name = "EMPTY"
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
