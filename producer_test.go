package kiroku

import (
	"context"
	"io"
	"os"
	"testing"
)

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
				t.Errorf("Producer.BatchBlock() error = %v, wantErr %v", err, tt.wantErr)
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
