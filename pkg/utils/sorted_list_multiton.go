//go:build sorted_list_multiton
// +build sorted_list_multiton

package utils

import "fmt"

// This file implements an Order Statistic Tree, which is a Red-Black Tree
// augmented with information about the size of the subtrees. This allows for
// efficient O(log n) retrieval of elements by their rank (index).
// This implementation supports duplicate keys.

const (
	red   = 1
	black = 0
)

// Item represents an item in the sorted list.
type Item struct {
	Key   int64
	Value interface{}
}

// Node represents a node in the Red-Black Tree.
type Node struct {
	Item
	left, right, parent *Node
	color               int
	size                int // Number of nodes in the subtree rooted at this node
}

// SortedList represents the Order Statistic Tree.
type SortedList struct {
	root *Node
	len  int
}

// NewSortedList creates a new SortedList.
func NewSortedList() *SortedList {
	return &SortedList{}
}

// Len returns the number of items in the list.
func (sl *SortedList) Len() int {
	return sl.len
}

// GetAll returns all values for a given key.
func (sl *SortedList) GetAll(key int64) []interface{} {
	var values []interface{}
	node := sl.findLeftmost(key)
	for node != nil && node.Key == key {
		values = append(values, node.Value)
		node = sl.successor(node)
	}
	return values
}

// GetByIndex returns the value at a given index in the sorted list.
func (sl *SortedList) GetByIndex(index int) (*Item, bool) {
	if index < 0 || index >= sl.len {
		return nil, false
	}
	node := sl.selectNode(sl.root, index)
	if node == nil {
		return nil, false
	}
	return &node.Item, true
}

// IndexOf returns the index of the first occurrence of a given key.
func (sl *SortedList) IndexOf(key int64) (int, bool) {
	node := sl.findLeftmost(key)
	if node == nil {
		return -1, false
	}
	return sl.rank(node), true
}

// Insert inserts a new key-value pair into the list.
func (sl *SortedList) Insert(key int64, value interface{}) {
	if sl.root == nil {
		sl.root = &Node{Item: Item{Key: key, Value: value}, color: black, size: 1}
		sl.len++
		return
	}

	parent := sl.findParentForInsert(key)
	newNode := &Node{Item: Item{Key: key, Value: value}, parent: parent, color: red, size: 1}
	if key < parent.Key {
		parent.left = newNode
	} else {
		parent.right = newNode
	}

	sl.fixSizesOnInsert(newNode)
	sl.insertFixup(newNode)
	sl.len++
}

// Delete removes one occurrence of an item by its key.
func (sl *SortedList) Delete(key int64) {
	node := sl.find(key)
	if node == nil {
		return // Not found
	}
	sl.deleteNode(node)
	sl.len--
}

// --- Internal implementation ---

func (sl *SortedList) find(key int64) *Node {
	current := sl.root
	for current != nil {
		if key == current.Key {
			return current
		}
		if key < current.Key {
			current = current.left
		} else {
			current = current.right
		}
	}
	return nil
}

func (sl *SortedList) findLeftmost(key int64) *Node {
	var result *Node
	current := sl.root
	for current != nil {
		if key < current.Key {
			current = current.left
		} else if key > current.Key {
			current = current.right
		} else { // key == current.Key
			result = current
			current = current.left
		}
	}
	return result
}

func (sl *SortedList) findParentForInsert(key int64) *Node {
	var parent *Node
	current := sl.root
	for current != nil {
		parent = current
		if key < current.Key {
			current = current.left
		} else { // key >= current.Key, go right for duplicates
			current = current.right
		}
	}
	return parent
}

func (sl *SortedList) rank(node *Node) int {
	r := size(node.left)
	y := node
	for y != sl.root {
		if y == y.parent.right {
			r += size(y.parent.left) + 1
		}
		y = y.parent
	}
	return r
}

func (sl *SortedList) selectNode(node *Node, i int) *Node {
	for node != nil {
		t := size(node.left)
		if i < t {
			node = node.left
		} else if i > t {
			node = node.right
			i = i - t - 1
		} else {
			return node
		}
	}
	return nil
}

// --- Size maintenance ---

func size(n *Node) int {
	if n == nil {
		return 0
	}
	return n.size
}

func (sl *SortedList) fixSizesOnInsert(node *Node) {
	current := parentOf(node)
	for current != nil {
		current.size++
		current = parentOf(current)
	}
}

func (sl *SortedList) fixSizesOnDelete(node *Node) {
	current := parentOf(node)
	for current != nil {
		current.size--
		current = parentOf(current)
	}
}

// --- Red-Black Tree rotations and fixups ---

func parentOf(n *Node) *Node {
	if n == nil {
		return nil
	}
	return n.parent
}

func grandparentOf(n *Node) *Node {
	return parentOf(parentOf(n))
}

func (sl *SortedList) leftRotate(x *Node) {
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		sl.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y

	y.size = x.size
	x.size = size(x.left) + size(x.right) + 1
}

func (sl *SortedList) rightRotate(y *Node) {
	x := y.left
	y.left = x.right
	if x.right != nil {
		x.right.parent = y
	}
	x.parent = y.parent
	if y.parent == nil {
		sl.root = x
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}
	x.right = y
	y.parent = x

	x.size = y.size
	y.size = size(y.left) + size(y.right) + 1
}

func (sl *SortedList) insertFixup(z *Node) {
	for z.parent != nil && z.parent.color == red {
		gp := grandparentOf(z)
		if z.parent == gp.left {
			y := gp.right // uncle
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				gp.color = red
				z = gp
			} else {
				if z == z.parent.right {
					z = z.parent
					sl.leftRotate(z)
				}
				z.parent.color = black
				grandparentOf(z).color = red
				sl.rightRotate(grandparentOf(z))
			}
		} else {
			y := gp.left // uncle
			if y != nil && y.color == red {
				z.parent.color = black
				y.color = black
				gp.color = red
				z = gp
			} else {
				if z == z.parent.left {
					z = z.parent
					sl.rightRotate(z)
				}
				z.parent.color = black
				grandparentOf(z).color = red
				sl.leftRotate(grandparentOf(z))
			}
		}
	}
	sl.root.color = black
}

func (sl *SortedList) deleteNode(z *Node) {
	var x, y *Node

	if z.left == nil || z.right == nil {
		y = z
	} else {
		y = sl.successor(z)
	}

	sl.fixSizesOnDelete(y)

	if y.left != nil {
		x = y.left
	} else {
		x = y.right
	}

	if x != nil {
		x.parent = y.parent
	}

	if y.parent == nil {
		sl.root = x
	} else if y == y.parent.left {
		y.parent.left = x
	} else {
		y.parent.right = x
	}

	if y != z {
		z.Key = y.Key
		z.Value = y.Value
	}

	if y.color == black {
		if x != nil {
			sl.deleteFixup(x)
		}
	}
}

func (sl *SortedList) deleteFixup(x *Node) {
	for x != sl.root && (x == nil || x.color == black) {
		if x == parentOf(x).left {
			w := parentOf(x).right // sibling
			if w != nil && w.color == red {
				w.color = black
				parentOf(x).color = red
				sl.leftRotate(parentOf(x))
				w = parentOf(x).right
			}
			if w != nil && (w.left == nil || w.left.color == black) && (w.right == nil || w.right.color == black) {
				w.color = red
				x = parentOf(x)
			} else if w != nil {
				if w.right == nil || w.right.color == black {
					if w.left != nil {
						w.left.color = black
					}
					w.color = red
					sl.rightRotate(w)
					w = parentOf(x).right
				}
				w.color = parentOf(x).color
				parentOf(x).color = black
				if w.right != nil {
					w.right.color = black
				}
				sl.leftRotate(parentOf(x))
				x = sl.root
			} else {
				x = parentOf(x)
			}
		} else {
			// same as then clause with "right" and "left" exchanged
			w := parentOf(x).left // sibling
			if w != nil && w.color == red {
				w.color = black
				parentOf(x).color = red
				sl.rightRotate(parentOf(x))
				w = parentOf(x).left
			}
			if w != nil && (w.right == nil || w.right.color == black) && (w.left == nil || w.left.color == black) {
				w.color = red
				x = parentOf(x)
			} else if w != nil {
				if w.left == nil || w.left.color == black {
					if w.right != nil {
						w.right.color = black
					}
					w.color = red
					sl.leftRotate(w)
					w = parentOf(x).left
				}
				w.color = parentOf(x).color
				parentOf(x).color = black
				if w.left != nil {
					w.left.color = black
				}
				sl.rightRotate(parentOf(x))
				x = sl.root
			} else {
				x = parentOf(x)
			}
		}
	}
	if x != nil {
		x.color = black
	}
}

func (sl *SortedList) successor(x *Node) *Node {
	if x.right != nil {
		return sl.minimum(x.right)
	}
	y := x.parent
	for y != nil && x == y.right {
		x = y
		y = y.parent
	}
	return y
}

func (sl *SortedList) minimum(x *Node) *Node {
	for x.left != nil {
		x = x.left
	}
	return x
}

func (sl *SortedList) String() string {
	return sl.inOrder(sl.root)
}

// Iter returns a channel that yields all items in the list in sorted order.
func (sl *SortedList) Iter() <-chan *Item {
	ch := make(chan *Item)
	go func() {
		sl.inOrderIter(sl.root, ch)
		close(ch)
	}()
	return ch
}

func (sl *SortedList) inOrderIter(n *Node, ch chan<- *Item) {
	if n == nil {
		return
	}
	sl.inOrderIter(n.left, ch)
	ch <- &n.Item
	sl.inOrderIter(n.right, ch)
}

func (sl *SortedList) inOrder(n *Node) string {
	if n == nil {
		return ""
	}
	s := sl.inOrder(n.left)
	s += fmt.Sprintf("{Key: %d, Size: %d} ", n.Key, n.size)
	s += sl.inOrder(n.right)
	return s
}
