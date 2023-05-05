# 🔥 CouloyDB & Kuloy 🔥
 
CouloyDB's goal is to compromise between performance and storage costs, as an alternative to memory KV storage like Redis in some scenarios.

## 🌟 What is CouloyDB & Kuloy?

**CouloyDB** is a fast KV store engine based on the bitcask model.

**Kuloy** is a KV storage service based on CouloyDB. It is compatible with the Redis protocol and supports consistent hash clustering and dynamic scaling.

**In a nutshell, Couloy is a code library that acts as an embedded storage engine like LevelDB, while Kuloy is a runnable program like Redis.**



## 🚀 How do I use CouloyDB & Kuloy?

> ⚠️ Note that CouloyDB & Kuloy has not been officially released and does not guarantee completely reliable compatibility!!!

### 🏁 Fast start: CouloyDB

Import the library:

```sh
go get github.com/Kirov7/CouloyDB
```

Use CouloyDB in your project:

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

### 🏁 Fast start: Kuloy

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

You can specify all configuration items through the configuration file or command-line parameters. If there is any conflict, the configuration file prevails. You need to modify the configuration file and do the same thing on each node. And make sure your port 7946 is bound.

`config.yaml`:

```yaml
yamlCopy codecluster:
  peers:
    - 192.168.1.151:9736
    - 192.168.1.152:9736
    - 192.168.1.153:9736
  self: 0
standalone:
  addr: "127.0.0.1:9736"
engine:
  dirPath: "/tmp/kuloy-test"
  dataFileSize: 268435456
  indexType: "btree"
  syncWrites: false
  mergeInterval: 28800
```

> You can find the configuration file template in cmd/config.

#### 🎯 Deploying standalone Kuloy service

```sh
./kuloy standalone -c ./config/config.yam
```

#### 🎯 Deploy consistent hash cluster Kuloy service

```sh
./kuloy cluster -c ./config/config.yaml
```

You can run the following command to view the functions of all configuration items:

```sh
./kuloy --help
```

The Kuloy service currently supports some operations of the String type in Redis, as well as some general operations.

You can use Kuloy as you would normally use Redis (only for currently supported operations, of course).

```sh
go get github.com/go-redis/redis/v8
```

```go
func TestKuloy(t *testing.T) {
	// Create a Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     "192.168.1.153:9736",
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

## 🔮 What will I do next?

- [x] Implement batch write with transaction semantics.
- [ ] Optimize hintfile storage structure to support the memtable build faster (may use gob).
- [ ] Increased use of flatbuffers build options to support faster reading speed.
- [x] Use mmap to read data file that on disk. [ however, the official mmap library is not optimized enough and needs to be further optimized ]
- [ ] Embedded lua script interpreter to support the execution of operations with complex logic.
- [x] Extend protocol support for Redis to act as a KV storage server in the network. [ has completed the basic implementation of Kuloy ]
- [ ] Extend to build complex data structures with the same interface as Redis, such as List, Hash, Set, ZSet, Bitmap, etc.
- [x] Extend easy-to-use distributed solution (may support both gossip and raft protocols for different usage scenarios) [ has supported gossip ]
- [ ] Extend to add backup nodes for a single node in a consistent hash cluster.
- [ ] Add the necessary Rehash functionality.

<br>
