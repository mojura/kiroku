package kiroku

import (
	"testing"
)

func TestOptions_validate(t *testing.T) {
	type testcase struct {
		o    Options
		want error
	}

	tcs := []testcase{
		{
			o: Options{
				Dir:  "./path/to/dir",
				Name: "myName",
			},
			want: nil,
		},
		{
			o: Options{
				Name: "myName",
			},
			want: ErrEmptyDirectory,
		},
		{
			o: Options{
				Dir: "./path/to/dir",
			},
			want: ErrEmptyName,
		},
	}

	for _, tc := range tcs {
		err := tc.o.Validate()
		if err != tc.want {
			t.Fatalf("invalid error, expected <%v> and received <%v>", tc.want, err)
		}
	}
}

func TestOptions_FullName(t *testing.T) {
	type fields struct {
		Name      string
		Namespace string
	}

	type testcase struct {
		name   string
		fields fields
		want   string
	}

	tests := []testcase{
		{
			name: "name",
			fields: fields{
				Name: "foo",
			},
			want: "foo",
		},
		{
			name: "name",
			fields: fields{
				Name:      "foo",
				Namespace: "bar",
			},
			want: "bar_foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &Options{
				Name:      tt.fields.Name,
				Namespace: tt.fields.Namespace,
			}

			if got := o.FullName(); got != tt.want {
				t.Errorf("Options.FullName() = %v, want %v", got, tt.want)
			}
		})
	}
}
