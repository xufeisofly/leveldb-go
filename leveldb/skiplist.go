package leveldb

import (
	"math/rand"
	"sync"
	"time"
)

// node is the node of skiplist
type node struct {
	key   []byte  // key bytes in a node
	nexts []*node // nexts nodes linked to this node, index represents the level

	mu *sync.RWMutex
}

func newNode(key []byte, height uint32) *node {
	return &node{
		key:   key,
		nexts: make([]*node, height),
		mu:    &sync.RWMutex{},
	}
}

func (node *node) Next(n uint32) *node {
	if n < 0 {
		panic("n is < 0")
	}
	node.mu.RLock()
	defer node.mu.RUnlock()

	return node.nexts[n]
}

func (node *node) SetNext(n uint32, x *node) {
	if n < 0 {
		panic("n is < 0")
	}
	node.mu.Lock()
	defer node.mu.Unlock()

	node.nexts[n] = x
}

func (node *node) NoBarrier_Next(n uint32) *node {
	if n < 0 {
		panic("n is < 0")
	}

	return node.nexts[n]
}

func (node *node) NoBarrier_SetNext(n uint32, x *node) {
	if n < 0 {
		panic("n is < 0")
	}

	node.nexts[n] = x
}

const kMaxHeight = 12

// skiplist is the core structure of memtable
type skiplist struct {
	comparator Comparator
	head       *node  // head node of the skiplist
	max_height uint32 // height of the entire list
}

func NewSkiplist(comparator Comparator) *skiplist {
	head := newNode(nil, kMaxHeight)
	for i := 0; i < kMaxHeight; i++ {
		head.SetNext(uint32(i), nil)
	}
	return &skiplist{
		comparator: comparator,
		head:       newNode(nil, kMaxHeight),
		max_height: 1,
	}
}

// Insert key into the list
// REQUIRES: nothing that compares equal to key is currently in the list
func (sl *skiplist) Insert(key []byte) {
	prev := make([]*node, kMaxHeight)
	x := sl.findGreatorOrEqual(key, &prev)

	if x == nil {
		panic("x not found")
	}
	if sl.Equal(key, x.key) {
		panic("do not allow duplicate insertion")
	}

	height := sl.randomHeight()
	if height > sl.getMaxHeight() {
		// extend the max height
		// fulfill new data of prev
		for i := sl.getMaxHeight(); i < height; i++ {
			prev[i] = sl.head
		}
	}

	x = newNode(key, height)
	for i := uint32(0); i < height; i++ {
		x.NoBarrier_SetNext(i, prev[i].NoBarrier_Next(i))
		prev[i].SetNext(i, x)
	}
}

// Contains returns true if an entry that compares equal to key is in the list
func (sl *skiplist) Contains(key []byte) bool {
	x := sl.findGreatorOrEqual(key, nil)
	return x != nil && sl.Equal(key, x.key)
}

const kBranching uint32 = 4

func (sl *skiplist) randomHeight() uint32 {
	rand.Seed(time.Now().UnixNano())
	height := uint32(1)
	for height < kMaxHeight && rand.Uint32()%kBranching == 0 {
		height += 1
	}
	if height <= 0 {
		panic("height <= 0")
	}
	if height > kMaxHeight {
		panic("height > kMaxHeight")
	}
	return height
}

func (sl *skiplist) getMaxHeight() uint32 {
	return sl.max_height
}

func (sl *skiplist) Equal(aKey, bKey []byte) bool {
	return sl.comparator.Compare(aKey, bKey) == 0
}

// keyIsAfterNode returns true if key is greater than the key stored in given node.
func (sl *skiplist) keyIsAfterNode(key []byte, n *node) bool {
	return n != nil && sl.comparator.Compare(key, n.key) > 0
}

// findGreatorOrEqual returns the earliest node that comes at or after the key.
// returns nil if there is no such node.
func (sl *skiplist) findGreatorOrEqual(key []byte, prev *[]*node) *node {
	curNode := sl.head
	level := sl.getMaxHeight() - 1
	for {
		next := curNode.Next(level)
		if sl.keyIsAfterNode(key, next) {
			// continue searching in this level
			curNode = next
		} else {
			// mark the level changing point
			if prev != nil {
				(*prev)[level] = curNode
			}
			if level == 0 {
				return next
			} else {
				level -= 1
			}
		}
	}
}

// findLessThan returns the latest node with a key < key
// returns head if there is no such node
func (sl *skiplist) findLessThan(key []byte) *node {
	curNode := sl.head
	level := sl.getMaxHeight() - 1

	for {
		next := curNode.Next(level)
		if next == nil || sl.comparator.Compare(next.key, key) < 0 {
			curNode = next
		} else {
			if level == 0 {
				return curNode
			} else {
				level -= 1
			}
		}
	}
}

// findLast returns the last node of skiplist
// returns head if list is empty
func (sl *skiplist) findLast() *node {
	curNode := sl.head
	level := sl.getMaxHeight() - 1

	for {
		next := curNode.Next(level)
		if next == nil {
			if level == 0 {
				return curNode
			} else {
				level -= 1
			}
		} else {
			curNode = next
		}
	}
}

// skiplistIterator
type skiplistIterator struct {
	list    *skiplist
	curNode *node
}

func NewSkiplistIterator(list *skiplist) *skiplistIterator {
	return &skiplistIterator{
		list:    list,
		curNode: nil,
	}
}

// Valid returns true if the iterator is positioned at a valid node.
func (iter *skiplistIterator) Valid() bool {
	return iter.curNode != nil
}

// Key returns the key at the current position.
// REQUIRES: Valid()
func (iter *skiplistIterator) Key() []byte {
	if !iter.Valid() {
		panic("node is not valid.")
	}
	return iter.curNode.key
}

// Next advances to the next position.
// REQUIRES: Valid()
func (iter *skiplistIterator) Next() {
	if !iter.Valid() {
		panic("node is not valid.")
	}
	iter.curNode = iter.curNode.Next(0)
}

// Prev advances to the previous position.
// REQUIRES: Valid()
func (iter *skiplistIterator) Prev() {
	if !iter.Valid() {
		panic("node is not valid.")
	}
	iter.curNode = iter.list.findLessThan(iter.curNode.key)
	if iter.curNode == iter.list.head {
		iter.curNode = nil
	}
}

// Seek advances to the first entry with a key >= target
func (iter *skiplistIterator) Seek(target []byte) {
	iter.curNode = iter.list.findGreatorOrEqual(target, nil)
}

// SeekToFirst advances to the first entry in the list.
func (iter *skiplistIterator) SeekToFirst() {
	iter.curNode = iter.list.head.Next(0)
}

// SeekToLast advances to the last entry in the list.
func (iter *skiplistIterator) SeekToLast() {
	iter.curNode = iter.list.findLast()
	if iter.curNode == iter.list.head {
		iter.curNode = nil
	}
}
