package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateCrc32(t *testing.T) {
	v1 := Crc32Value([]byte("abcdef"))

	v2 := Crc32Value([]byte("abc"))
	v3 := Crc32ValueWithInitial(v2, []byte("def"))

	assert.Equal(t, v1, v3)
}
