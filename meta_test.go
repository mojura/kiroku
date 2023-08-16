package kiroku

import (
	"testing"
)

func TestMeta_merge_nil(t *testing.T) {
	var m Meta
	m.merge(nil)
}
