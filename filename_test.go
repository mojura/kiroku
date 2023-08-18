package kiroku

import (
	"testing"
)

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
