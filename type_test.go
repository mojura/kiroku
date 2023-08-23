package kiroku

import (
	"bytes"
	"testing"
)

func TestType_Validate(t *testing.T) {
	type testcase struct {
		name    string
		tr      Type
		wantErr bool
	}

	tests := []testcase{
		{
			name:    "chunk",
			tr:      TypeChunk,
			wantErr: false,
		},
		{
			name:    "snapshot",
			tr:      TypeSnapshot,
			wantErr: false,
		},
		{
			name:    "temporary",
			tr:      TypeTemporary,
			wantErr: false,
		},
		{
			name:    "invalid",
			tr:      TypeTemporary + 100,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.tr.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Type.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestType_String(t *testing.T) {
	type testcase struct {
		name string
		tr   Type
		want string
	}

	tests := []testcase{
		{
			name: "chunk",
			tr:   TypeChunk,
			want: "chunk",
		},
		{
			name: "snapshot",
			tr:   TypeSnapshot,
			want: "snapshot",
		},
		{
			name: "temporary",
			tr:   TypeTemporary,
			want: "tmp",
		},
		{
			name: "invalid",
			tr:   TypeTemporary + 100,
			want: "INVALID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if str := tt.tr.String(); str != tt.want {
				t.Errorf("Type.String() value = %v, want %v", str, tt.want)
			}
		})
	}
}

func TestType_MarshalJSON(t *testing.T) {
	type testcase struct {
		name string

		want    []byte
		wantErr bool

		tr Type
	}

	tests := []testcase{
		{
			name:    "chunk",
			tr:      TypeChunk,
			want:    []byte(`"chunk"`),
			wantErr: false,
		},
		{
			name:    "snapshot",
			tr:      TypeSnapshot,
			want:    []byte(`"snapshot"`),
			wantErr: false,
		},
		{
			name:    "temporary",
			tr:      TypeTemporary,
			want:    []byte(`"tmp"`),
			wantErr: false,
		},
		{
			name:    "invalid",
			tr:      TypeTemporary + 100,
			want:    []byte(`"INVALID"`),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bs, err := tt.tr.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("Type.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !bytes.Equal(bs, tt.want) {
				t.Errorf("Type.MarshalJSON() value = %v, want %v", string(bs), string(tt.want))
			}
		})
	}
}

func TestType_UnmarshalJSON(t *testing.T) {
	type args struct {
		bs []byte
	}

	type testcase struct {
		name string

		args args

		want    Type
		wantErr bool
	}

	tests := []testcase{
		{
			name: "chunk",
			args: args{
				bs: []byte(`"chunk"`),
			},
			want:    TypeChunk,
			wantErr: false,
		},
		{
			name: "snapshot",
			args: args{
				bs: []byte(`"snapshot"`),
			},
			want:    TypeSnapshot,
			wantErr: false,
		},
		{
			name: "temporary",
			args: args{
				bs: []byte(`"tmp"`),
			},
			want:    TypeTemporary,
			wantErr: false,
		},
		{
			name: "invalid",
			args: args{
				bs: []byte(`"INVALID"`),
			},
			want:    TypeInvalid,
			wantErr: true,
		},
		{
			name: "parse error",
			args: args{
				bs: []byte(`1`),
			},
			want:    TypeInvalid,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var tester Type
			err := tester.UnmarshalJSON(tt.args.bs)
			if (err != nil) != tt.wantErr {
				t.Errorf("Type.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}

			if tester != tt.want {
				t.Errorf("Type.UnmarshalJSON() value = %v, want %v", tester, tt.want)
			}
		})
	}
}
