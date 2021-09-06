package kiroku

import (
	"testing"
	"time"
)

func TestMirrorOptions_validate(t *testing.T) {
	type testcase struct {
		o    MirrorOptions
		want error
	}

	tcs := []testcase{
		{
			o: MirrorOptions{
				Options: Options{
					Dir:  "./path/to/dir",
					Name: "myName",
				},
			},
			want: nil,
		},
		{
			o: MirrorOptions{
				Options: Options{
					Dir:  "./path/to/dir",
					Name: "myName",
				},
				EndOfResultsDelay: time.Second,
			},
			want: nil,
		},
		{
			o: MirrorOptions{
				Options: Options{
					Name: "myName",
				},
			},
			want: ErrEmptyDirectory,
		},
		{
			o: MirrorOptions{
				Options: Options{
					Dir: "./path/to/dir",
				},
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
