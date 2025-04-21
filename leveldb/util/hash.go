package util

import "github.com/spaolacci/murmur3"

func Hash(data []byte, seed uint32) uint32 {
	return murmur3.Sum32WithSeed(data, seed)
}
