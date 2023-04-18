# CouloyDB
## CouloyDB is a fast KV store engine based on bitcask model
<br>
CouloyDB's goal is to compromise between performance and storage costs, as some scenarios redis alternative products.

## What will I do next ?
<br>
- [x] Implement batch write with transaction semantics.
- [ ] Optimize hintfile storage structure to support the memtable build faster (may use gob).
- [ ] Increased use of flatbuffers build options to support faster reading speed.
- [ ] Use mmap to read data file that on disk.
- [ ] Embedded lua script interpreter to support the execution of operations with complex logic.
- [ ] Extend to build complex data structures with the same interface as Redis, such as List, Hash, Set, ZSet, Bitmap, etc.
- [ ] Extend protocol support for Redis to act as a KV storage server in the network.
- [ ] Extend easy to use distributed solution (may support both gossip and raft protocols for different usage scenarios)

<br>
> why called Couloy?
> Couloy is a game character whose job is a fire assassin with high mobility and practical value<br>
> Thanks to this character, I passed many levels, and thanks to this game, as a reason to get up early every day during my most confused period<br>
> You can find the other character in my other project https://github.com/Kirov7/LabrysCache
