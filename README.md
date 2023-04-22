# CouloyDB  (In Developing)
## CouloyDB is a fast KV store engine based on bitcask model
CouloyDB's goal is to compromise between performance and storage costs, as an alternative to Redis in some scenarios.

## Fast start
import the library
```sh
go get github.com/Kirov7/CouloyDB
```
<br>
use CouloyDB in your project

```go
import(
	couloy "github.com/Kirov7/CouloyDB"
	"github.com/Kirov7/CouloyDB/meta"
	"log"
)

func main() {
	opt := couloy.Options{
		DirPath:      "/tmp/couloy",
		DataFileSize: 4096,
		IndexerType:  meta.BTree,
		SyncWrites:   true,
	}
	db, err := couloy.NewCouloyDB(opt)
	if err != nil {
		log.Fatal(err)
	}
	
	// Be careful, you can't use single non-displayable character in ASCII code as your key (0x00 ~ 0x1F and 0x7F),
	// because those characters will be used in CouloyDB as necessary operations in the preset key tagging system.
	key := []byte("first key")
	value := []byte("first value")
	
	err = db.Put(key, value)
	if err != nil {
		log.Fatal(err)
	}

	v, err := db.Get(v)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Del(v)
	if err != nil {
		log.Fatal(err)
	}
}
```
<br>

## What will I do next ?

- [x] Implement batch write with transaction semantics.
- [ ] Optimize hintfile storage structure to support the memtable build faster (may use gob).
- [ ] Increased use of flatbuffers build options to support faster reading speed.
- [ ] Use mmap to read data file that on disk.
- [ ] Embedded lua script interpreter to support the execution of operations with complex logic.
- [ ] Extend to build complex data structures with the same interface as Redis, such as List, Hash, Set, ZSet, Bitmap, etc.
- [ ] Extend protocol support for Redis to act as a KV storage server in the network.
- [ ] Extend easy to use distributed solution (may support both gossip and raft protocols for different usage scenarios)

<br>

> why called Couloy ?<br>
> Couloy is a game character whose job is a fire assassin with high mobility and practical value<br>
> Thanks to this character, I passed many levels, and thanks to this game, as a reason to get up early every day during my most confused period<br>
> You can find the other character in my other project https://github.com/Kirov7/LabrysCache
