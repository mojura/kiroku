package kiroku

import (
	"fmt"
	"testing"
)

func TestType_Validate(t *testing.T) {
	type testcase struct {
		t   Type
		err string
	}

	tcs := []testcase{
		{
			t: TypeWriteAction,
		},
		{
			t: TypeDeleteAction,
		},
		{
			t: TypeComment,
		},
		{
			t:   TypeComment + 1,
			err: fmt.Sprintf(invalidTypeLayout, TypeComment+1),
		},
	}

	for _, tc := range tcs {
		err := tc.t.Validate()
		switch {
		case err == nil && tc.err == "":
		case err != nil && tc.err == err.Error():

		case err == nil && tc.err != "":
			t.Fatalf("invalid error, expected <%s> and received nil", tc.err)
		case err != nil && tc.err != err.Error():
			t.Fatalf("invalid error, expected <%s> and received <%v>", tc.err, err)
		}
	}
}

func TestType_String(t *testing.T) {
	type testcase struct {
		t   Type
		str string
	}

	tcs := []testcase{
		{
			t:   TypeWriteAction,
			str: "write",
		},
		{
			t:   TypeDeleteAction,
			str: "delete",
		},
		{
			t:   TypeComment,
			str: "comment",
		},
		{
			t:   TypeComment + 1,
			str: "invalid",
		},
	}

	for _, tc := range tcs {
		if str := tc.t.String(); str != tc.str {
			t.Fatalf("invalid string value, expected <%s> and received <%s>", tc.str, str)
		}
	}
}
