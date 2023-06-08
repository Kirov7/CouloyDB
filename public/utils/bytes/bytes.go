package bytes

import (
	"fmt"
	"golang.org/x/exp/rand"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func IntToBytes(x int) []byte {
	return []byte(fmt.Sprintf("%d", x))
}

func RandomBytes(length int) []byte {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return b
}
