package bytex

import (
	"fmt"
	"golang.org/x/exp/rand"
	"sort"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GetTestKey(x int) []byte {
	return []byte(fmt.Sprintf("%09d", x))
}

func RandomBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}

func BytesSort(data [][]byte) {
	sort.Slice(data, func(i, j int) bool {
		return string(data[i]) < string(data[j])
	})
}
