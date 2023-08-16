package kiroku

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/mojura/enkodo"
)

func TestBlock_Marshal_Unmarshal_Enkodo(t *testing.T) {
	type args struct {
		enc *enkodo.Encoder
	}

	type testcase struct {
		name    string
		b       Block
		args    args
		wantErr bool
	}

	tests := []testcase{
		{
			name:    "basic",
			b:       Block("hello world"),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBuffer(nil)
			enc := enkodo.NewWriter(buf)
			if err := enc.Encode(tt.b); (err != nil) != tt.wantErr {
				t.Errorf("enkodo.Encoder.Encode() error = %v, wantErr %v", err, tt.wantErr)
			}

			dec := enkodo.NewReader(buf)

			var got Block
			if err := dec.Decode(&got); (err != nil) != tt.wantErr {
				t.Errorf("enkodo.Decoder.Decode() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !reflect.DeepEqual(tt.b, got) {
				t.Errorf("invalid value: got = %v, want %v", string(got), string(tt.b))
			}
		})
	}
}

func TestBlock_MarshalEnkodo(t *testing.T) {
	type args struct {
		enc *enkodo.Encoder
	}
	tests := []struct {
		name    string
		b       Block
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.b.MarshalEnkodo(tt.args.enc); (err != nil) != tt.wantErr {
				t.Errorf("Block.MarshalEnkodo() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
