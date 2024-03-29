package kiroku

import (
	"encoding/json"
	"fmt"
)

const (
	TypeInvalid Type = iota
	TypeChunk
	TypeSnapshot
	TypeTemporary
)

func parseType(str string) (t Type, err error) {
	switch str {
	case "chunk":
		t = TypeChunk
	case "snapshot":
		t = TypeSnapshot
	case "tmp":
		t = TypeTemporary
	default:
		err = fmt.Errorf("type of <%s> is not supported", str)
	}

	return
}

type Type uint8

func (t Type) Validate() (err error) {
	switch t {
	case TypeChunk:
	case TypeSnapshot:
	case TypeTemporary:

	default:
		return fmt.Errorf("invalid filetype, <%s> is not supported", t)
	}

	return
}

func (t Type) String() (out string) {
	switch t {
	case TypeChunk:
		return "chunk"
	case TypeSnapshot:
		return "snapshot"
	case TypeTemporary:
		return "tmp"

	default:
		return "INVALID"
	}
}

func (t Type) MarshalJSON() (bs []byte, err error) {
	return json.Marshal(t.String())
}

func (t *Type) UnmarshalJSON(bs []byte) (err error) {
	var str string
	if err = json.Unmarshal(bs, &str); err != nil {
		return
	}

	var val Type
	if val, err = parseType(str); err != nil {
		return
	}

	*t = val
	return
}
