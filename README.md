# CouloyDB & Kuloy

English | [简体中文](https://github.com/Kirov7/CouloyDB/blob/master/README_ZH.md)

CouloyDB's goal is to compromise between performance and storage costs, as an alternative to memory KV storage like Redis in some scenarios.

![icon.png](https://img1.imgtp.com/2023/05/05/WZgs6o2t.png)

## 🌟 What is CouloyDB & Kuloy?

**CouloyDB** is a fast KV store engine based on the bitcask model.

**Kuloy** is a KV storage service based on CouloyDB. It is compatible with the Redis protocol and supports consistent hash clustering and dynamic scaling.

**In a nutshell, Couloy is a code library that acts as an embedded storage engine like LevelDB, while Kuloy is a runnable program like Redis.**

## 🚀 How to use CouloyDB & Kuloy?

> ⚠️ **Notice:** CouloyDB & Kuloy has not been officially released and does not guarantee completely reliable compatibility!!!

### 🏁 Fast start: CouloyDB

Import the library:

```sh
go get github.com/Kirov7/CouloyDB
```

#### Basic usage example

Now, the basic API of CouloyDB only supports key-value pairs of **simple byte array type**. These APIs only can guarantee **the atomicity of a single operation**. If you only want to store some simple data, you can use these APIs.They may be abolition soon, so **we recommend that all operations be done in a transaction**. And they **should not** be used concurrently with transaction APIs, which would break the ACID properties of transactions.

```go
func TestCouloyDB(t *testing.T) {
	conf := couloy.DefaultOptions()
	db, err := couloy.NewCouloyDB(conf)
	if err != nil {
		log.Fatal(err)
	}

	key := []byte("first key")
	value := []byte("first value")

	// Be careful, you can't use single non-displayable character in ASCII code as your key (0x00 ~ 0x1F and 0x7F),
	// because those characters will be used in CouloyDB as necessary operations in the preset key tagging system.
	// This may be changed in the next major release
	err = db.Put(key, value)
	if err != nil {
		log.Fatal(err)
	}

	v, err := db.Get(key)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v)

	err = db.Del(v)
	if err != nil {
		log.Fatal(err)
	}

	keys := db.ListKeys()
	for _, k := range keys {
		fmt.Println(k)
	}

	target := []byte("target")
	err = db.Fold(func(key []byte, value []byte) bool {
		if bytes.Equal(value, target) {
			fmt.Println("Include target")
			return false
		}
		return true
	})
	if err != nil {
		log.Fatal(err)
	}
}
```
#### Transaction usage example

Transaction should be used if you want operations to be **safe** and store data in a **different data structure**.Currently, transaction support **read-committed** isolation levels and **serializable** isolation levels.Since the Bitcask model requires a full storage index, it is not planned to implement MVCC to support snapshot isolation level or serializable snapshot isolation level.

```go
func TestTxn(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}

	// the first parameter passed in is used to determine whether to automatically retry 
	// when this transaction conflicts
	err = db.RWTransaction(false, func(txn *Txn) error {
		return txn.Set(bytex.GetTestKey(0), bytex.GetTestKey(0))
	})

	if err != nil {
		log.Fatal(err)
	}

	// the first parameter passed in is used to determine whether this transaction is read-only
	err = db.SerialTransaction(true, func(txn *Txn) error {
		value, err := txn.Get(bytex.GetTestKey(0))
		if err != nil {
			return err
		}
		if !bytes.Equal(value, bytex.GetTestKey(0)) {
			return err
		}
		return nil
	})

	if err != nil {
		log.Fatal(err)
	}
}
```
You can safely manipulate the database by calling methods of `Txn`.

#### Currently supported data structure types and supported operations：

- String:
  - GET
  - SET
  - DEL
  - SETNX
  - GETSET
  - STRLEN
  - INCR
  - INCRBY
  - DECR
  - DECRBY
  - EXIST
  - APPEND
  - MGET
  - MSET
- Hash:
  - HSET
  - HGET
  - HDEL
  - HEXIST
  - HGETALL
  - HMSET
  - HMGET
  - HLEN
  - HVALUES
  - HKEYS
  - HSTRLEN
- List:
  - LPUSH
  - RPUSH
  - LPOP
  - RPOP


In the future, we will support more data structures and operations.

### 🏁 Fast start: Kuloy

> ⚠️ **Notice:** We are **refactoring** Kuloy so that it directly calls the native interface of the storage engine layer (CouloyDB) to implement commands. After the refactoring is completed, we will open a new repository to maintain Kuloy. Now it only supports less features.

You can download executable files directly or compile through source code:

```sh
# Compile through source code
git clone https://github.com/Kirov7/CouloyDB

cd ./CouloyDB/cmd

go mod tidy

go build run.go

mv run kuloy
# Next, you can see the executable file named kuloy in the cmd directory
```

Then you can deploy quickly through configuration files or command-line arguments.

You can specify all configuration items through the configuration file or command-line parameters. If there is any conflict, the configuration file prevails. You need to modify the configuration file and do the same thing on each node. And make sure your port 7946 is bound (be used for synchronous cluster status).

`config.yaml`:

```yaml
cluster:
  # all the nodes in the cluster (including itself)
  peers:
    - 192.168.1.151:9736
    - 192.168.1.152:9736
    - 192.168.1.153:9736
  # local Index in the cluster peers
  self: 0
standalone:
  # address of this instance when standalone deployment
  addr: "127.0.0.1:9736"
engine:
  # directory Path where data logs are stored
  dirPath: "/tmp/kuloy-test"
  # maximum byte size per datafile (unit: Byte)
  dataFileSize: 268435456
  # type of memory index (hashmap/btree/art)
  indexType: "btree"
  # whether to enable write synchronization
  syncWrites: false
  # periods for data compaction (unit: Second)
  mergeInterval: 28800
```

You can find the configuration file template in cmd/config.

#### 🎯 Deploying standalone Kuloy service

```sh
./kuloy standalone -c ./config/config.yam
```

#### 🎯 Deploy consistent hash cluster Kuloy service

```sh
./kuloy cluster -c ./config/config.yaml
```

#### 🎯 View Help Options

You can run the following command to view the functions of all configuration items:

```sh
./kuloy --help
```

#### 🎯 Accessing Kuloy service

The Kuloy service currently supports some operations of the String type in Redis, as well as some general operations.More data structures will be supported after our refactoring is complete.

You can use Kuloy as you would normally use Redis (only for currently supported operations, of course).

Use the go-redis client demonstration here:

```sh
go get github.com/go-redis/redis/v8
```

```go
func TestKuloy(t *testing.T) {
	// Create a Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:9736",
		DB:       0,  // Kuloy supports db selection
	})

	// Test set operation
	err := client.Set(context.Background(), "mykey", "hello world", 0).Err()
	if err != nil {
		log.Fatal(err)
	}

	// Test get operation
	val, err := client.Get(context.Background(), "mykey").Result()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("mykey ->", val)
}
```

#### 📜 Currently supported commands

- Key
  - DEL
  - EXISTS
  - KEYS
  - FLUSHDB
  - TYPE
  - RENAME
  - RENAMENX
- String
  - GET
  - SET
  - SETNX
  - GETSET
  - STRLEN
- Connection
  - PING
  - SELECT

## 🔮 What will we do next?

- [x] Implement batch write and basic transaction functions [ now, CouloyDB supports the RC and the Serializable transaction isolation level ].
- [ ] Optimize hintfile storage structure to support the memtable build faster (may use gob).
- [ ] Increased use of flatbuffers build options to support faster reading speed.
- [x] Use mmap to read data file that on disk. [ however, the official mmap library is not optimized enough and needs to be further optimized ]
- [x] Embedded lua script interpreter to support the execution of operations with complex logic [ currently implemented on CouloyDB, Kuloy still does not support ].
- [x] Extend protocol support for Redis to act as a KV storage server in the network. [ has completed the basic implementation of Kuloy ]
- [ ] Extend to build data structures in storage engine layer with the same interface as Redis:
  - [x] String
  - [x] Hash
  - [x] List
  - [ ] Set
  - [ ] ZSet
  - [ ] Bitmap

- [x] Extend easy-to-use distributed solution (may support both gossip and raft protocols for different usage scenarios) [ has supported gossip ]
- [ ] Extend to add backup nodes for a single node in a consistent hash cluster.
- [ ] Add the necessary Rehash functionality.

## Contact us?
If you have any questions and want to contact us, you can send an email to: crazyfay@qq.com.

Or join this Tencent WeChat group. We will try our best to solve all the problems.

<img src="https://img1.imgtp.com/2023/07/18/bKL5QgLr.png" alt="WeChat.png" style="width:20%;" />
