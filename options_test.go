package kiroku

import "testing"

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
