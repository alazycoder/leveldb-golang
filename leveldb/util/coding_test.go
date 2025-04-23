package util

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUint32(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	data := make([]byte, 8)

	for idx := 0; idx < 10000; idx++ {
		clear(data)
		value := rnd.Uint32()
		encodeLength := VarIntLength(uint64(value))
		EncodeVarInt32(data, value)
		decodeValue, decodeLength := DecodeVarInt32(data)
		assert.Equal(t, value, decodeValue)
		assert.Equal(t, encodeLength, decodeLength)
	}
}
