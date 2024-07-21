package promsketch

import (
	"fmt"
)

// https://pkg.go.dev/container/heap#pkg-constants

// TODO: define Item type by our sketch needs!
type Item struct {
	key   string
	count int64 // The value of the item; arbitrary.
	// The index is needed by update and is maintained by the heap.Interface methods.
}

// A minHeap implements heap.Interface and holds Items.
type minHeap []*Item

// TopK implementation
type TopKHeap struct {
	heap      minHeap
	k         int
	key_index map[string]int // map a key to index in the heap
	totalMem  float64        // Bytes
}

func (topkheap *TopKHeap) InitKeyIndex() {
	topkheap.key_index = make(map[string]int)
	for i, item := range topkheap.heap {
		topkheap.key_index[item.key] = i
	}
}

func (topkheap *TopKHeap) Clean() {
	clear(topkheap.key_index)
	topkheap.heap = topkheap.heap[:0]
}

func (topkheap *TopKHeap) GetMemoryBytes() float64 {
	return topkheap.totalMem*2 + 12 // Bytes
}

func NewTopKHeap(k int) (topkheap *TopKHeap) {
	topkheap = &TopKHeap{
		heap:      make(minHeap, 0, k),
		k:         k,
		totalMem:  0,
		key_index: make(map[string]int),
	}
	return topkheap
}

func NewTopKFromHeap(from *TopKHeap) (topkheap *TopKHeap) {
	topkheap = &TopKHeap{
		k:         from.k,
		key_index: make(map[string]int),
		heap:      make(minHeap, len(from.heap)),
	}
	copy(topkheap.heap, from.heap)
	// Didn't copy key_index here for performance consideration; if needed, should add for correctness.

	return topkheap
}

func (topkheap *TopKHeap) Print() {
	for _, item := range topkheap.heap {
		fmt.Println(item.key, ":", item.count, " ", topkheap.key_index[item.key])
	}
}

// update modifies the priority and value of an Item in the queue.
func (topkheap *TopKHeap) Update(key string, count int64) bool {
	//if topkheap.key_index == nil {
	// topkheap.InitKeyIndex()
	//}

	index, find := topkheap.key_index[key]

	if find == true {
		topkheap.heap[index].count = count
		topkheap.updateOrder(index)
		return true
	} else {
		topkheap.Insert(key, count)
		return true
	}
}

func (topkheap *TopKHeap) Insert(key string, count int64) {
	if int(len(topkheap.heap)) < topkheap.k {
		topkheap.heap = append(topkheap.heap, &Item{
			key:   key,
			count: count,
		})
		topkheap.totalMem += float64(len(key)) + 12
		topkheap.key_index[key] = len(topkheap.heap) - 1
		topkheap.updateOrderUp(len(topkheap.heap) - 1)
	} else {
		if topkheap.heap[0].count < count {
			topkheap.heap[0].count = count
			topkheap.heap[0].key = key
			topkheap.key_index[key] = 0
			if topkheap.k > 1 {
				topkheap.updateOrderDown(0)
			}
		}
	}
}

func (topkheap *TopKHeap) updateOrder(i int) {
	if !topkheap.updateOrderDown(i) {
		topkheap.updateOrderUp(i)
	}
}

func (topkheap *TopKHeap) updateOrderDown(i int) bool {
	n := len(topkheap.heap)
	i0 := i
	var (
		l, r, smallest int = 0, 0, 0
	)
	for i < n {
		l = 2*i + 1
		r = 2*i + 2
		smallest = i

		if l < n && topkheap.heap[smallest].count > topkheap.heap[l].count {
			smallest = l
		}
		if r < n && topkheap.heap[smallest].count > topkheap.heap[r].count {
			smallest = r
		}

		if smallest != i {
			topkheap.key_index[topkheap.heap[smallest].key], topkheap.key_index[topkheap.heap[i].key] = topkheap.key_index[topkheap.heap[i].key], topkheap.key_index[topkheap.heap[smallest].key]
			topkheap.heap[smallest], topkheap.heap[i] = topkheap.heap[i], topkheap.heap[smallest]
		} else {
			break
		}
		i = smallest
	}
	return i > i0
}

func (topkheap *TopKHeap) updateOrderUp(i int) {
	var par int = 0
	for i > 0 {
		par = (i - 1) / 2
		if topkheap.heap[par].count > topkheap.heap[i].count {
			topkheap.key_index[topkheap.heap[par].key], topkheap.key_index[topkheap.heap[i].key] = topkheap.key_index[topkheap.heap[i].key], topkheap.key_index[topkheap.heap[par].key]
			topkheap.heap[par], topkheap.heap[i] = topkheap.heap[i], topkheap.heap[par]
			i = par
		} else {
			break
		}
	}
}
