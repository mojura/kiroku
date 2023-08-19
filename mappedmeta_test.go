package kiroku

import (
	"os"
	"testing"
)

func Test_newMappedMeta(t *testing.T) {
	type args struct {
		opts Options

		avoidDirectory     bool
		setInvalidMetaSize bool
	}

	type testcase struct {
		name    string
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name:    "basic",
			args:    args{opts: MakeOptions("./testing", "test")},
			wantErr: false,
		},
		{
			name:    "missing directory",
			args:    args{opts: MakeOptions("./testing", "test"), avoidDirectory: true},
			wantErr: true,
		},
		{
			name:    "invalid meta size",
			args:    args{opts: MakeOptions("./testing", "test"), setInvalidMetaSize: true},
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

			if tt.args.setInvalidMetaSize {
				oldMeta := metaSize
				metaSize = -1
				defer func() { metaSize = oldMeta }()
			}
			_, err := newMappedMeta(tt.args.opts)
			if (err != nil) != tt.wantErr {
				t.Errorf("newMappedMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_mappedMeta_Get(t *testing.T) {
	type fields struct {
		opts Options

		setClosed bool
	}

	type testcase struct {
		name   string
		fields fields

		wantMeta Meta
	}

	tests := []testcase{
		{
			name:   "basic",
			fields: fields{opts: MakeOptions("./testing", "test")},
		},
		{
			name:   "closed",
			fields: fields{opts: MakeOptions("./testing", "test"), setClosed: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			m, err := newMappedMeta(tt.fields.opts)
			if err != nil {
				t.Fatal(err)
			}

			if tt.fields.setClosed {
				_ = m.Close()
			}

			meta := m.Get()
			if meta != tt.wantMeta {
				t.Errorf("mappedmeta.Get() error = %v, wantErr %v", meta, tt.wantMeta)
				return
			}
		})
	}
}

func Test_mappedMeta_Set(t *testing.T) {
	type fields struct {
		opts Options

		setClosed bool
	}

	type args struct {
		meta Meta
	}

	type testcase struct {
		name   string
		fields fields
		args   args
	}

	tests := []testcase{
		{
			name: "basic",
			args: args{
				meta: Meta{LastProcessedTimestamp: 12345, LastProcessedType: TypeSnapshot},
			},
			fields: fields{opts: MakeOptions("./testing", "test")},
		},
		{
			name:   "closed",
			fields: fields{opts: MakeOptions("./testing", "test"), setClosed: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			m, err := newMappedMeta(tt.fields.opts)
			if err != nil {
				t.Fatal(err)
			}

			if tt.fields.setClosed {
				_ = m.Close()
			}

			m.Set(tt.args.meta)

			if tt.fields.setClosed {
				return
			}

			if err = m.Close(); err != nil {
				t.Fatal(err)
			}

			if m, err = newMappedMeta(tt.fields.opts); err != nil {
				t.Fatal(err)
			}

			if *m.m != tt.args.meta {
				t.Fatalf("mappedmeta.Get() error = %v, wantErr %v", *m.m, tt.args.meta)
			}
		})
	}
}

func Test_mappedMeta_Close(t *testing.T) {
	type fields struct {
		opts Options

		setClosed       bool
		nilMemoryMap    bool
		alreadyUnmapped bool
	}

	type testcase struct {
		name   string
		fields fields

		wantErr bool
	}

	tests := []testcase{
		{
			name:   "basic",
			fields: fields{opts: MakeOptions("./testing", "test")},
		},
		{
			name:    "closed",
			fields:  fields{opts: MakeOptions("./testing", "test"), setClosed: true},
			wantErr: true,
		},
		{
			name:    "nil memory map",
			fields:  fields{opts: MakeOptions("./testing", "test"), nilMemoryMap: true},
			wantErr: false,
		},
		{
			name:    "already unmapped",
			fields:  fields{opts: MakeOptions("./testing", "test"), alreadyUnmapped: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := os.Mkdir(tt.fields.opts.Dir, 0744); err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(tt.fields.opts.Dir)

			m, err := newMappedMeta(tt.fields.opts)
			if err != nil {
				t.Fatal(err)
			}

			if tt.fields.setClosed {
				_ = m.Close()
			}

			if tt.fields.nilMemoryMap {
				m.mm = nil
			}

			if tt.fields.alreadyUnmapped {
				_ = m.mm.Unmap()
				m.mm = []byte{}
			}

			if err = m.Close(); (err != nil) != tt.wantErr {
				t.Errorf("mappedmeta.Close() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
