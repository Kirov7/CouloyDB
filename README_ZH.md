# CouloyDB & Kuloy

CouloyDB的目标是在某些场景中作为内存KV存储（例如Redis）的替代品，并在性能和存储成本之间取得平衡。

![icon.png](https://img1.imgtp.com/2023/05/05/WZgs6o2t.png)

## 🌟 什么是 CouloyDB 和 Kuloy？

**CouloyDB** 是一个基于 bitcask 模型的快速 KV 存储引擎。

**Kuloy** 是基于 CouloyDB 的 KV 存储服务。它与 Redis 协议兼容，并支持一致性哈希分片和动态扩展。

**简而言之，Couloy 就像 LevelDB 那样作为嵌入式存储引擎的代码库，而 Kuloy 则像 Redis 一样作为可执行程序。**

## 🚀 如何使用 CouloyDB 和 Kuloy？

> ⚠️ **注意：** CouloyDB 和 Kuloy尚未正式发布，无法完全保证可靠的兼容性！

### 🏁 快速上手 CouloyDB

引入库：

```sh
go get github.com/Kirov7/CouloyDB
```

#### 基础使用案例

目前，CouloyDB的基本 API 仅支持**简单的字节数组类型**的键值对。这些 API 只能保证**单个操作的原子性**。如果您只想存储一些简单的数据，可以使用这些 API。然而，这些API可能很快就会被废弃，所以**我们建议所有操作都在事务中完成**。请注意，它们**不应该**与事务 API 并发使用，因为这可能会破坏事务的 ACID 特性。

```go
func TestCouloyDB(t *testing.T) {
	conf := couloy.DefaultOptions()
	db, err := couloy.NewCouloyDB(conf)
	if err != nil {
		log.Fatal(err)
	}

	key := []byte("first key")
	value := []byte("first value")

	// 请注意，您不能在ASCII码中使用单个不可显示字符作为您的键（0x00 ~ 0x1F和0x7F）
	// 因为这些字符在CouloyDB中将用于预设的键标记系统中的必要操作
	// 在下一个重要更新中，这一点可能会有所改变
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

#### 事务使用案例

如果您希望操作是**安全的**，并且将数据存储在**不同的数据结构**中，应该使用事务。目前，事务支持**读取提交**（read-committed）隔离级别和**可串行化**（serializable）隔离级别。由于 Bitcask 模型需要完整的存储索引，因此我们不打算实现 MVCC 以支持快照隔离级别或可串行化快照隔离级别。

```go
func TestTxn(t *testing.T) {
	db, err := NewCouloyDB(DefaultOptions())
	if err != nil {
		log.Fatal(err)
	}

	// 传入的第一个参数用于确定在事务冲突时是否自动进行重试。
	err = db.RWTransaction(false, func(txn *Txn) error {
		return txn.Set(bytex.GetTestKey(0), bytex.GetTestKey(0))
	})

	if err != nil {
		log.Fatal(err)
	}

	// 传入的第一个参数用于确定这个事务是否是只读事务
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

你可以通过使用 `Txn` 的方法来安全的操作数据库。

#### 当前支持的数据结构及支持的操作：

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
- List:
  - LPUSH
  - RPUSH
  - LPOP
  - RPOP


未来我们将会支持更多的数据结构和操作。

### 🏁 快速上手 Kuloy

> ⚠️ **注意：**我们正在对 Kuloy 进行**重构**，以便直接调用存储引擎层（CouloyDB）的本地接口来实现命令。重构完成后，我们将开设一个新的仓库来维护 Kuloy。现在它仅支持较少的功能。

您可以直接下载可执行文件，或者通过源代码进行编译：

```sh
# 通过源代码进行编译
git clone https://github.com/Kirov7/CouloyDB

cd ./CouloyDB/cmd

go mod tidy

go build run.go

mv run kuloy
# 接下来，您可以在cmd目录中看到名为kuloy的可执行文件
```

然后，您可以通过配置文件或命令行参数快速部署。

您可以通过配置文件或命令行参数指定所有配置项。如果有任何冲突，配置文件优先生效。您需要修改配置文件，并在每个节点上执行相同的操作。同时确保您的端口7946被绑定（用于同步集群状态）。

`config.yaml`：

```yaml
cluster:
  # 所有集群中的节点（包括自身）
  peers:
    - 192.168.1.151:9736
    - 192.168.1.152:9736
    - 192.168.1.153:9736
  # 在集群对等节点中的本地索引（local index）
  self: 0
standalone:
  # 独立部署时此实例的地址
  addr: "127.0.0.1:9736"
engine:
  # 数据日志存储的目录路径
  dirPath: "/tmp/kuloy-test"
  # 每个数据文件的最大字节大小（单位：字节）
  dataFileSize: 268435456
  # 内存索引的类型（hashmap、btree、ART）
  indexType: "btree"
  # 是否启用写入同步
  syncWrites: false
  # 数据压缩的时间间隔（单位：秒）
  mergeInterval: 28800
```

您可以在cmd/config目录中找到配置文件的模板。

#### 🎯部署独立的Kuloy服务

```sh
./kuloy standalone -c ./config/config.yam
```

#### 🎯 部署一致性哈希集群的 Kuloy 服务

```sh
./kuloy cluster -c ./config/config.yaml
```

#### 🎯 查看帮助选项

您可以运行以下命令来查看所有配置项的功能：

```
./kuloy --help
```

#### 🎯 访问 Kuloy 服务

Kuloy 服务目前支持 Redis 中String 类型的一些操作，以及一些通用操作。在我们重构完成之后会支持更多数据结构。

您可以像通常使用 Redis 一样使用 Kuloy（当然，仅适用于当前支持的操作）。

使用此处的 go-redis 客户端演示：

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

#### 📜 现在支持的命令

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

## 🔮 接下来我们会做什么？

- [x] 实现批量写入和基本事务功能（目前 CouloyDB 支持 RC 和 Serialized 事务隔离级别）。
- [ ] 优化 hintfile 存储结构以支持更快地构建 memtable（可能会使用 gob）。
- [ ] 增加使用 flatbuffers 构建选项来支持更快的读取速度。
- [x] 使用 mmap 读取磁盘上的数据文件（不过官方的 mmap 库性能不够好，需要进一步优化）。
- [x] 内嵌 lua 脚本解释器，支持复杂逻辑操作的执行（目前在 CouloyDB 上实现，Kuloy 还不支持）。
- [ ] 扩展对 Redis 的协议支持，使其充当网络中的 KV 存储服务器（已完成 Kuloy 的基本实现）。
- [ ] 扩展构建存储引擎层的数据结构，接口与 Redis 相同：
  - [x] String
  - [x] Hash
  - [x] List
  - [ ] Set
  - [ ] ZSet
  - [ ] Bitmap
- [ ] 扩展易用的分布式解决方案：
  - [ ] Raft
  - [x] Gossip
- [ ] 为一致性哈希集群中的单个节点添加备份节点。
- [ ] 添加必要的 Rehash 功能。

## 联系我们

如果您有任何疑问并想联系我们，您可以发送电子邮件至：crazyfay@qq.com。或者加入这个腾讯微信群。 我们将尽力解决所有问题。

<img src="https://img1.imgtp.com/2023/07/18/bKL5QgLr.png" alt="WeChat.png" style="width:20%;" />