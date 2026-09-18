// Package deque implements a double-ended queue on top of a ring buffer.
//
// Elements live in a slice that is treated as a circle: head marks where
// the first element is, and the rest follow it, wrapping around from the
// last slot back to slot 0. Pushing to the front just moves head one step
// back, so no element ever has to shift. That makes pushes and pops at
// both ends O(1), and so is reading the i-th element, which a linked list
// can't offer.
//
//	capacity 8, head = 6, contents Z A B C:
//	slot:  0   1   2   3   4   5   6   7
//	     [ B | C | . | . | . | . | Z | A ]
//	                               ^ head
package deque

// minCapacity is the size of the buffer allocated on first push.
const minCapacity = 8

// Deque is a double-ended queue. The zero value is an empty deque ready to
// use. It is not safe for concurrent use; the store's lock protects it.
type Deque[T any] struct {
	buf  []T
	head int // index of the first element in buf
	n    int // number of elements
}

func (d *Deque[T]) Len() int { return d.n }

func (d *Deque[T]) PushBack(v T) {
	d.growIfFull()
	d.buf[d.slot(d.n)] = v
	d.n++
}

func (d *Deque[T]) PushFront(v T) {
	d.growIfFull()
	d.head = (d.head - 1 + len(d.buf)) % len(d.buf)
	d.buf[d.head] = v
	d.n++
}

// PopFront removes and returns the first element. ok is false if the deque
// is empty.
func (d *Deque[T]) PopFront() (v T, ok bool) {
	if d.n == 0 {
		return v, false
	}
	v = d.buf[d.head]
	d.clear(d.head)
	d.head = (d.head + 1) % len(d.buf)
	d.n--
	d.shrinkIfSparse()
	return v, true
}

// PopBack removes and returns the last element. ok is false if the deque
// is empty.
func (d *Deque[T]) PopBack() (v T, ok bool) {
	if d.n == 0 {
		return v, false
	}
	last := d.slot(d.n - 1)
	v = d.buf[last]
	d.clear(last)
	d.n--
	d.shrinkIfSparse()
	return v, true
}

// At returns the i-th element, counting from the front. It panics if i is
// out of range, like indexing a slice does.
func (d *Deque[T]) At(i int) T {
	if i < 0 || i >= d.n {
		panic("deque: index out of range")
	}
	return d.buf[d.slot(i)]
}

// slot converts a position counted from the front into an index in buf.
func (d *Deque[T]) slot(i int) int {
	return (d.head + i) % len(d.buf)
}

// clear zeroes a slot so the buffer doesn't keep a popped value (and
// whatever it points to) alive for the garbage collector.
func (d *Deque[T]) clear(i int) {
	var zero T
	d.buf[i] = zero
}

func (d *Deque[T]) growIfFull() {
	if d.n < len(d.buf) {
		return
	}
	d.resize(max(2*len(d.buf), minCapacity))
}

// shrinkIfSparse halves the buffer once it is only a quarter full, so a
// list that once held a million elements doesn't keep that memory forever.
// Shrinking at a quarter rather than at half avoids thrashing when the size
// hovers around a power of two.
func (d *Deque[T]) shrinkIfSparse() {
	if len(d.buf) > minCapacity && d.n <= len(d.buf)/4 {
		d.resize(len(d.buf) / 2)
	}
}

// resize copies the elements, in order, into a new buffer of the given
// capacity, starting at slot 0.
func (d *Deque[T]) resize(capacity int) {
	buf := make([]T, capacity)
	for i := range d.n {
		buf[i] = d.buf[d.slot(i)]
	}
	d.buf = buf
	d.head = 0
}
