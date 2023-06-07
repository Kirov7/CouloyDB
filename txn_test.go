package CouloyDB

import (
	"bytes"
	"github.com/Kirov7/CouloyDB/public"
	"github.com/Kirov7/CouloyDB/public/utils/wait"
	"log"
	"strconv"
	"testing"
	"time"
)

func TestDB_SerialTransaction(t *testing.T) {
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

	w := wait.NewWait()

	_ = db.RWTransaction(false, writeLoop1)

	w.Add(1)
	go func() {
		_ = db.SerialTransaction(true, readLoop1)
		w.Done()
	}()

	w.Add(1)
	go func() {
		_ = db.SerialTransaction(false, writeLoop2)
		w.Done()
	}()

	w.Add(1)
	go func() {
		_ = db.SerialTransaction(true, readLoop2)
		w.Done()
	}()

	w.Wait()
	_ = db.Close()
}

func TestDB_SerialTransaction_With_RWTransaction(t *testing.T) {
	conf := DefaultOptions()
	db, _ := NewCouloyDB(conf)

	writeAciont1 := func(txn *Txn) error {
		time.Sleep(time.Second)
		err := txn.Put([]byte("key"), []byte("hello"))
		if err != nil {
			return nil
		}
		return nil
	}

	writeAciont2 := func(txn *Txn) error {
		err := txn.Put([]byte("key"), []byte("world"))
		if err != nil {
			return nil
		}
		return nil
	}

	w := wait.NewWait()

	w.Add(1)
	go func() {
		time.Sleep(100 * time.Millisecond) // Ensure the serializable transaction have started executing
		err := db.RWTransaction(false, writeAciont1)
		// Because the serializable transaction has been committed before the RW transaction commits
		// So this RW transaction will definitely fail to commit due to conflicts
		if err != public.ErrTransactionConflict {
			t.Fail()
		}
		w.Done()
	}()
	go func() {
		_ = db.SerialTransaction(false, writeAciont2)
		w.Done()
	}()

	w.Wait()
	v, _ := db.Get([]byte("key"))
	if bytes.Compare(v, []byte("world")) != 0 {
		t.Fail()
	}
}
