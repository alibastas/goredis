package deque

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// contents returns the elements of d from front to back.
func contents[T any](d *Deque[T]) []T {
	out := make([]T, d.Len())
	for i := range out {
		out[i] = d.At(i)
	}
	return out
}

func TestPushAndPopBothEnds(t *testing.T) {
	var d Deque[string]

	d.PushBack("b")
	d.PushBack("c")
	d.PushFront("a")
	if got, want := contents(&d), []string{"a", "b", "c"}; !slices.Equal(got, want) {
		t.Fatalf("contents = %v, want %v", got, want)
	}

	if v, ok := d.PopFront(); !ok || v != "a" {
		t.Fatalf("PopFront = %q, %v; want a, true", v, ok)
	}
	if v, ok := d.PopBack(); !ok || v != "c" {
		t.Fatalf("PopBack = %q, %v; want c, true", v, ok)
	}
	if v, ok := d.PopBack(); !ok || v != "b" {
		t.Fatalf("PopBack = %q, %v; want b, true", v, ok)
	}
	if _, ok := d.PopFront(); ok {
		t.Fatal("PopFront on empty deque returned ok")
	}
	if _, ok := d.PopBack(); ok {
		t.Fatal("PopBack on empty deque returned ok")
	}
}

func TestWrapAroundAndGrowth(t *testing.T) {
	var d Deque[int]
	// Pushing to the front of an empty buffer wraps head around to the
	// last slot straight away; growing past 8 then has to unwrap it.
	for i := range 20 {
		d.PushFront(i)
	}
	want := make([]int, 20)
	for i := range want {
		want[i] = 19 - i
	}
	if got := contents(&d); !slices.Equal(got, want) {
		t.Fatalf("contents = %v, want %v", got, want)
	}
}

func TestShrinksAfterManyPops(t *testing.T) {
	var d Deque[int]
	for i := range 1000 {
		d.PushBack(i)
	}
	for range 990 {
		d.PopFront()
	}
	if c := len(d.buf); c > 64 {
		t.Fatalf("buffer capacity is still %d after draining to 10 elements", c)
	}
	if got, want := contents(&d), []int{990, 991, 992, 993, 994, 995, 996, 997, 998, 999}; !slices.Equal(got, want) {
		t.Fatalf("contents = %v, want %v", got, want)
	}
}

func TestAtPanicsOutOfRange(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("At(1) on a one-element deque did not panic")
		}
	}()
	var d Deque[int]
	d.PushBack(1)
	d.At(1)
}

// TestMatchesSliceModel runs thousands of random operations against both
// the deque and a plain slice doing the same thing the slow way. Any
// difference means a bug in the index arithmetic.
func TestMatchesSliceModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2)) // fixed seed: failures are reproducible
	var d Deque[int]
	var model []int

	for step := range 20000 {
		switch op := rng.IntN(4); op {
		case 0:
			d.PushFront(step)
			model = slices.Insert(model, 0, step)
		case 1:
			d.PushBack(step)
			model = append(model, step)
		case 2:
			got, ok := d.PopFront()
			if len(model) == 0 {
				if ok {
					t.Fatalf("step %d: PopFront on empty deque returned %d", step, got)
				}
				continue
			}
			if !ok || got != model[0] {
				t.Fatalf("step %d: PopFront = %d, %v; want %d", step, got, ok, model[0])
			}
			model = model[1:]
		case 3:
			got, ok := d.PopBack()
			if len(model) == 0 {
				if ok {
					t.Fatalf("step %d: PopBack on empty deque returned %d", step, got)
				}
				continue
			}
			if last := model[len(model)-1]; !ok || got != last {
				t.Fatalf("step %d: PopBack = %d, %v; want %d", step, got, ok, last)
			}
			model = model[:len(model)-1]
		}

		if d.Len() != len(model) {
			t.Fatalf("step %d: Len = %d, want %d", step, d.Len(), len(model))
		}
	}
	if got := contents(&d); !slices.Equal(got, model) {
		t.Fatalf("final contents differ:\n got: %v\nwant: %v", got, model)
	}
}
