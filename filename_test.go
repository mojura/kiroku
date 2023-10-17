package kiroku

import (
	"reflect"
	"testing"
)

func Test_ParseFilename(t *testing.T) {
	type args struct {
		filename string
	}

	type testcase struct {
		name       string
		args       args
		wantParsed Filename
		wantErr    bool
	}

	tests := []testcase{
		{
			name: "basic",
			args: args{
				filename: "test.12345.snapshot.kir",
			},
			wantParsed: Filename{
				name:      "test",
				createdAt: 12345,
				filetype:  TypeSnapshot,
			},
			wantErr: false,
		},
		{
			name: "not enough parts",
			args: args{
				filename: "test.12345.kir",
			},
			wantParsed: Filename{},
			wantErr:    true,
		},
		{
			name: "invalid created at",
			args: args{
				filename: "test.foo.snapshot.kir",
			},
			wantParsed: Filename{},
			wantErr:    true,
		},
		{
			name: "error parsing filetype",
			args: args{
				filename: "test.12345.11.kir",
			},
			wantParsed: Filename{
				name:      "test",
				createdAt: 12345,
			},
			wantErr: true,
		},
		{
			name: "invalid filetype",
			args: args{
				filename: "test.12345.INVALID.kir",
			},
			wantParsed: Filename{
				name:      "test",
				createdAt: 12345,
				filetype:  TypeInvalid,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotParsed, err := ParseFilename(tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseFilename() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(gotParsed, tt.wantParsed) {
				t.Errorf("ParseFilename() = %v, want %v", gotParsed, tt.wantParsed)
			}
		})
	}
}

func TestFilename_String(t *testing.T) {
	type fields struct {
		name      string
		createdAt int64
		filetype  Type
	}

	type testcase struct {
		name   string
		fields fields
		want   string
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				name:      "test",
				createdAt: 12345,
				filetype:  TypeSnapshot,
			},
			want: "test.12345.snapshot.kir",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := makeFilename(tt.fields.name, tt.fields.createdAt, tt.fields.filetype)
			if got := f.String(); got != tt.want {
				t.Errorf("Filename.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFilename_toMeta(t *testing.T) {
	type fields struct {
		name      string
		createdAt int64
		filetype  Type
	}

	type testcase struct {
		name   string
		fields fields
		want   Meta
	}

	tests := []testcase{
		{
			name: "basic",
			fields: fields{
				name:      "test",
				createdAt: 12345,
				filetype:  TypeSnapshot,
			},
			want: Meta{LastProcessedTimestamp: 12345, LastProcessedType: TypeSnapshot},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := makeFilename(tt.fields.name, tt.fields.createdAt, tt.fields.filetype)
			if got := f.toMeta(); got != tt.want {
				t.Errorf("Filename.toMeta() = %v, want %v", got, tt.want)
			}
		})
	}
}
