package kiroku

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"time"
)

/*
	func Test_watcher_process(t *testing.T) {
		type fields struct {
			ctx       context.Context
			onTrigger func(Filename) error
			s         semaphore
			opts      Options
			jobs      sync.WaitGroup
		}
		type args struct {
			targetPrefix string
		}
		tests := []struct {
			name    string
			fields  fields
			args    args
			wantOk  bool
			wantErr bool
		}{
			// TODO: Add test cases.
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				w := &watcher{
					ctx:       tt.fields.ctx,
					out:       tt.fields.out,
					onTrigger: tt.fields.onTrigger,
					s:         tt.fields.s,
					opts:      tt.fields.opts,
					jobs:      tt.fields.jobs,
				}
				gotOk, err := w.process(tt.args.targetPrefix)
				if (err != nil) != tt.wantErr {
					t.Errorf("watcher.process() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if gotOk != tt.wantOk {
					t.Errorf("watcher.process() = %v, want %v", gotOk, tt.wantOk)
				}
			})
		}
	}
*/

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
