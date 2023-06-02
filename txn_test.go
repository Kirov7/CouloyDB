package CouloyDB

import (
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestSerializableTxn(t *testing.T) {
	conf := DefaultOptions()
	db, err := NewCouloyDB(conf)
	if err != nil {
		log.Fatal(err)
	}

	readLoop1 := func(txn *Txn) error {
		for i := 1; i < 11; i++ {
			v, _ := txn.Get([]byte(strconv.Itoa(i)))
			log.Printf("Read key: %v, value: %v in read loop1", i, string(v))
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	readLoop2 := func(txn *Txn) error {
		for i := 1; i < 11; i++ {
			v, _ := txn.Get([]byte(strconv.Itoa(i)))
			log.Printf("Read key: %v, value: %v in read loop2", i, string(v))
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}

	writeLoop1 := func(txn *Txn) error {
		for i := 1; i <= 10; i++ {
			err := txn.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			if err != nil {
				return err
			}
			log.Printf("Write key: %v, value: %v in write loop1", i, i)
		}
		return nil
	}

	writeLoop2 := func(txn *Txn) error {
		for i := 11; i <= 20; i++ {
			err := txn.Put([]byte(strconv.Itoa(i)), []byte(strconv.Itoa(i)))
			if err != nil {
				return err
			}
			log.Printf("Write key: %v, value: %v in write loop2", i, i)
		}
		return nil
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		_ = db.ReadOnlyTransaction(readLoop1)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		_ = db.ReadOnlyTransaction(readLoop2)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		_ = db.RWTransaction(true, writeLoop1)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		_ = db.RWTransaction(true, writeLoop2)
		wg.Done()
	}()

	wg.Wait()
	_ = db.Close()
}
