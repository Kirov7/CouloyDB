package bytex

import (
	"encoding/binary"
	"fmt"
	"sort"

	"golang.org/x/exp/rand"
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

func ConcatBytes(a, b []byte) []byte {
	return append(a, b...)
}

func EncodeByteSlices(key, value []byte) []byte {
	header := make([]byte, binary.MaxVarintLen64*2)
	var index int
	index += binary.PutVarint(header[index:], int64(len(key)))
	index += binary.PutVarint(header[index:], int64(len(value)))
	length := len(key) + len(value)
	buf := make([]byte, length+index)
	copy(buf[:index], header[:index])
	copy(buf[index:index+len(key)], key)
	copy(buf[index+len(key):], value)
	return buf
}

func DecodeByteSlices(data []byte) ([]byte, []byte) {
	var index int
	dataSize, i := binary.Varint(data[index:])
	index += i
	_, i = binary.Varint(data[index:])
	index += i
	sep := index + int(dataSize)
	return data[index:sep], data[sep:]
}
