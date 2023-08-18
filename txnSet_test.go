package CouloyDB

import (
	"reflect"
	"sort"
	"testing"

	"github.com/Kirov7/CouloyDB/public"
	"github.com/stretchr/testify/assert"
)

func TestTxnSADD(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	type args struct {
		key     []byte
		members [][]byte
	}
	type want struct {
		Error    error
		expected [][]byte
	}
	tests := []struct {
		Name string
		Args args
		Want want
	}{
		{
			Name: "Set Single Member",
			Args: args{
				key: []byte("singleMember"),
				members: [][]byte{
					[]byte("hoge"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
				},
			},
		},
		{
			Name: "Set Multible Member",
			Args: args{
				key: []byte("saddMultipleMember"),
				members: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
		},
		{
			Name: "Set Duplicate Member",
			Args: args{
				key: []byte("saddDuplicateMember"),
				members: [][]byte{
					[]byte("java"),
					[]byte("java"),
					[]byte("golang"),
					[]byte("golang"),
					[]byte("rust"),
					[]byte("rust"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
		},
		{
			Name: "Set Empty Key",
			Args: args{
				key: []byte(""),
				members: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
			Want: want{
				Error:    public.ErrKeyIsEmpty,
				expected: [][]byte{},
			},
		},
		{
			Name: "Set Empty Member",
			Args: args{
				key: []byte("saddEmptyMember"),
				members: [][]byte{
					[]byte(""),
				},
			},
			Want: want{
				Error:    public.ErrKeyIsEmpty,
				expected: [][]byte{},
			},
		},
	}
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			err := txn.SADD(test.Args.key, test.Args.members...)
			return err
		})
		assert.Equal(t, test.Want.Error, err)
	}
}

func TestTxnSREM(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	type args struct {
		key        []byte
		members    [][]byte
		remMembers [][]byte
	}
	type want struct {
		Error    error
		expected [][]byte
	}
	tests := []struct {
		Name string
		Args args
		Want want
	}{
		{
			Name: "REM Single Member",
			Args: args{
				key: []byte("sremSingleMember"),
				members: [][]byte{
					[]byte("hoge"),
				},
				remMembers: [][]byte{
					[]byte("hoge"),
				},
			},
			Want: want{
				expected: [][]byte{},
			},
		},
		{
			Name: "REM Multible Member",
			Args: args{
				key: []byte("sremMultipleMember"),
				members: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
				remMembers: [][]byte{
					[]byte("java"),
					[]byte("golang"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("rust"),
				},
			},
		},
		{
			Name: "REM Not Exist Member",
			Args: args{
				key: []byte("sremNotExistMember"),
				members: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
				remMembers: [][]byte{
					[]byte("python"),
				},
			},
			Want: want{
				Error: public.ErrKeyNotFound,
				expected: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
		},
	}
	// prepare the data
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			err := txn.SADD(test.Args.key, test.Args.members...)
			return err
		})
		assert.Nil(t, err)
	}

	// execute SREM
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			err := txn.SREM(test.Args.key, test.Args.remMembers...)
			return err
		})
		assert.Equal(t, err, test.Want.Error)
	}

	// check the data
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			members, err := txn.SMEMBERS(test.Args.key)
			ok := compareSlices(members, test.Want.expected)
			assert.True(t, ok)
			return err
		})
		assert.Nil(t, err)
	}
}

func TestTxnSCARD(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	type args struct {
		key     []byte
		members [][]byte
	}
	type want struct {
		Error    error
		expected [][]byte
	}
	tests := []struct {
		Name string
		Args args
		Want want
	}{
		{
			Name: "Set Single Member",
			Args: args{
				key: []byte("scardSingleMember"),
				members: [][]byte{
					[]byte("hoge"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
				},
			},
		},
		{
			Name: "Set Multible Member",
			Args: args{
				key: []byte("scardMultipleMember"),
				members: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
		},
		{
			Name: "Set Duplicate Member",
			Args: args{
				key: []byte("scardDuplicateMember"),
				members: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
		},
	}
	// prepare the data
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			err := txn.SADD(test.Args.key, test.Args.members...)
			return err
		})
		assert.Nil(t, err)
	}

	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			count, err := txn.SCARD(test.Args.key)
			assert.Equal(t, len(test.Want.expected), int(count))
			return err
		})
		assert.Nil(t, err)
	}
}

func TestTxnSMEMBER(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	assert.Nil(t, err)
	assert.NotNil(t, db)
	defer destroyCouloyDB(db)

	type args struct {
		key     []byte
		members [][]byte
	}
	type want struct {
		Error    error
		expected [][]byte
	}
	tests := []struct {
		Name string
		Args args
		Want want
	}{
		{
			Name: "Set Single Member",
			Args: args{
				key: []byte("smemebreSingleMember"),
				members: [][]byte{
					[]byte("hoge"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
				},
			},
		},
		{
			Name: "Set Multible Member",
			Args: args{
				key: []byte("smemebreMultipleMember"),
				members: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("hoge"),
					[]byte("fuga"),
					[]byte("piyo"),
				},
			},
		},
		{
			Name: "Set Duplicate Member",
			Args: args{
				key: []byte("smemberDuplicateMember"),
				members: [][]byte{
					[]byte("java"),
					[]byte("java"),
					[]byte("golang"),
					[]byte("golang"),
					[]byte("rust"),
					[]byte("rust"),
				},
			},
			Want: want{
				expected: [][]byte{
					[]byte("java"),
					[]byte("golang"),
					[]byte("rust"),
				},
			},
		},
	}
	// prepare the data
	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			err := txn.SADD(test.Args.key, test.Args.members...)
			return err
		})
		assert.Nil(t, err)
	}

	for _, test := range tests {
		err = db.SerialTransaction(false, func(txn *Txn) error {
			members, err := txn.SMEMBERS(test.Args.key)
			ok := compareSlices(members, test.Want.expected)
			assert.True(t, ok)
			return err
		})
		assert.Nil(t, err)
	}
}

// byteSlices represents a type for a slice of byte slices.
type byteSlices [][]byte

func (p byteSlices) Len() int           { return len(p) }
func (p byteSlices) Less(i, j int) bool { return string(p[i]) < string(p[j]) }
func (p byteSlices) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// compareSlices checks if two slices of byte slices (a and b)have the same elements, regardless of order.
//
// It returns true if they have the same elements, false otherwise.
func compareSlices(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}

	sort.Sort(byteSlices(a))
	sort.Sort(byteSlices(b))

	return reflect.DeepEqual(a, b)
}
