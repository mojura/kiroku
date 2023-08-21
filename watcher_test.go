package kiroku

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"time"
)

func Test_watcher_getNext(t *testing.T) {
	type fields struct {
		opts       Options
		targetType Type
		prep       func() error
	}

	type args struct {
		filename string
		info     os.FileInfo
	}

	type testcase struct {
		name   string
		fields fields
		args   args

		wantFilename Filename
		wantOk       bool
		wantErr      bool
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				opts:       MakeOptions("./testing", "testing"),
				targetType: TypeChunk,
				prep: func() (err error) {
					var f *os.File
					if f, err = os.Create("./testing/testing.12346.chunk.kir"); err != nil {
						return
					}
					_ = f.Close()
					return
				},
			},
			args: args{
				filename: "testing.12345.chunk.kir",
				info:     &mockFileInfo{},
			},
			wantFilename: Filename{
				name:      "testing",
				createdAt: 12346,
				filetype:  TypeChunk,
			},
			wantOk:  true,
			wantErr: false,
		},
		{
			name: "contains unrelated file",
			fields: fields{
				opts:       MakeOptions("./testing", "testing"),
				targetType: TypeChunk,
				prep: func() (err error) {
					var f *os.File
					if f, err = os.Create("./testing/foobar.12346.chunk.kir"); err != nil {
						return
					}
					_ = f.Close()

					if f, err = os.Create("./testing/testing.12346.chunk.kir"); err != nil {
						return
					}
					_ = f.Close()
					return
				},
			},
			args: args{
				filename: "testing.12345.chunk.kir",
				info:     &mockFileInfo{},
			},
			wantFilename: Filename{
				name:      "testing",
				createdAt: 12346,
				filetype:  TypeChunk,
			},
			wantOk:  true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			if err := tt.fields.prep(); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())

			w := newWatcher(ctx, tt.fields.opts, func(f Filename) error { return nil }, tt.fields.targetType)
			defer w.waitToComplete()
			defer cancel()

			gotFilename, gotOk, gotErr := w.getNext()
			if gotFilename != tt.wantFilename {
				t.Errorf("watcher.getNext() = %v, want %v", gotFilename, tt.wantFilename)
			}

			if gotOk != tt.wantOk {
				t.Errorf("watcher.getNext() = %v, want %v", gotOk, tt.wantOk)
			}

			if (gotErr != nil) != tt.wantErr {
				t.Errorf("NewConsumer() error = %v, wantErr %v", gotErr, tt.wantErr)
				return
			}
		})
	}
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
