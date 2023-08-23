package kiroku

import (
	"os"
	"testing"
	"time"
)

func Test_newWriter(t *testing.T) {
	type args struct {
		dir      string
		filename Filename
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
				dir:      "./",
				filename: makeFilename("foo", time.Now().UnixNano(), TypeTemporary),
			},
			wantErr: false,
		},
		{
			name: "error opening file",
			args: args{
				dir:      "./does-not-exist",
				filename: makeFilename("foo", time.Now().UnixNano(), TypeTemporary),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := newWriter(tt.args.dir, tt.args.filename)
			if err == nil {
				defer os.Remove(w.filepath)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("newWriter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestWriter_Write(t *testing.T) {
	type fields struct {
		filename  Filename
		isClosed  bool
		closeFile bool
	}

	type args struct {
		value Block
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
				filename: makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
			},
			args: args{
				value: Block("hello"),
			},
			wantErr: false,
		},
		{
			name: "closed",
			fields: fields{
				filename: makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
				isClosed: true,
			},
			args: args{
				value: Block("hello"),
			},
			wantErr: true,
		},
		{
			name: "closed file",
			fields: fields{
				filename:  makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
				closeFile: true,
			},
			args: args{
				value: Block("hello"),
			},
			wantErr: true,
		},
		{
			name: "closed file",
			fields: fields{
				filename: makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
			},
			args: args{
				value: Block(""),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := newWriter("./", tt.fields.filename)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(w.filepath)

			w.closed = tt.fields.isClosed

			if tt.fields.closeFile {
				w.f.Close()
			}

			if err := w.Write(tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("Writer.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWriter_Close(t *testing.T) {
	type fields struct {
		filename Filename
		isClosed bool
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
				filename: makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
			},
			wantErr: false,
		},
		{
			name: "already closed",
			fields: fields{
				filename: makeFilename("temp", time.Now().UnixNano(), TypeTemporary),
				isClosed: true,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w, err := newWriter("./", tt.fields.filename)
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(w.filepath)

			w.closed = tt.fields.isClosed
			if err := w.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Writer.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
