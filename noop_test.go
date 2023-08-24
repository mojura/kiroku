package kiroku

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestNOOP_Export(t *testing.T) {
	type args struct {
		prefix   string
		filename string
		r        io.Reader
	}

	type testcase struct {
		name string
		args args

		wantFilename string
		wantErr      bool
	}

	tests := []testcase{
		{
			args: args{
				filename: "test",
				r:        strings.NewReader("hello world"),
			},
			wantFilename: "test",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NOOP{}
			gotFilename, err := n.Export(context.Background(), tt.args.prefix, tt.args.filename, tt.args.r)
			if gotFilename != tt.wantFilename {
				t.Errorf("NOOP.Export() filename = %v, wantFiename %v", gotFilename, tt.wantFilename)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("NOOP.Export() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNOOP_Import(t *testing.T) {
	type args struct {
		prefix   string
		filename string
		w        io.Writer
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			args: args{
				filename: "test",
				w:        bytes.NewBuffer(nil),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NOOP{}
			if err := n.Import(context.Background(), tt.args.prefix, tt.args.filename, tt.args.w); (err != nil) != tt.wantErr {
				t.Errorf("NOOP.Export() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNOOP_Get(t *testing.T) {
	type args struct {
		prefix   string
		filename string
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			args: args{
				filename: "test",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NOOP{}
			if err := n.Get(context.Background(), tt.args.prefix, tt.args.filename, func(r io.Reader) error {
				return nil
			}); (err != nil) != tt.wantErr {
				t.Errorf("NOOP.Export() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNOOP_GetNext(t *testing.T) {
	type args struct {
		prefix       string
		lastFilename string
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			args: args{
				prefix:       "test",
				lastFilename: "test_000",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NOOP{}
			if _, err := n.GetNext(context.Background(), tt.args.prefix, tt.args.lastFilename); (err != nil) != tt.wantErr {
				t.Errorf("NOOP.Export() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
