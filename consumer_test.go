package kiroku

import (
	"context"
	"io"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestConsumer_Meta(t *testing.T) {
	type fields struct {
		opts     Options
		src      Source
		onUpdate func(*Reader) error
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(r *Reader) (err error) {
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
		onUpdate   func(*Reader) error
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(r *Reader) (err error) {
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) { return "", nil },
				),
				onUpdate: func(r *Reader) (err error) {
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
		onUpdate func(*Reader) error
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
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
				onUpdate: func(r *Reader) (err error) {
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
		onUpdate func(*Reader) error
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return nil },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return nil },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(r *Reader) (err error) {
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return os.ErrNotExist },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return os.ErrNotExist },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(r *Reader) (err error) {
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
					func(ctx context.Context, filename string, r io.Reader) error { return nil },
					func(ctx context.Context, filename string, w io.Writer) error { return io.EOF },
					func(ctx context.Context, filename string, fn func(io.Reader) error) error { return io.EOF },
					func(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
						return "", nil
					},
				),
				onUpdate: func(r *Reader) (err error) {
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

			if err = c.getLatestSnapshot(); (err != nil) != tt.wantErr {
				t.Errorf("Consumer.getLatestSnapshot() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
