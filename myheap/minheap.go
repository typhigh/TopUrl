package myheap

import "fmt"

// HeapNode is the node of MinHeap
type HeapNode struct {
	url   string
	count int
}

// Init function set up HeapNode's url and count
func (node *HeapNode) Init(url string, count int) {
	node.url = url
	node.count = count
}

// Get url
func (node *HeapNode) GetURL() string {
	return node.url
}

// Get count
func (node *HeapNode) GetCount() int {
	return node.count
}

// NewHeapNode function make new heap node with given url and count
func NewHeapNode(url string, count int) HeapNode {
	var node HeapNode
	node.Init(url, count)
	return node
}

// MinHeap is the min root heap, cap may be very small
type MinHeap struct {
	cap   int
	nodes []HeapNode
}

// GetCap function return MinHeap's cap
func (heap *MinHeap) GetCap() int {
	return heap.cap
}

// GetNode function retun the node of MinHeap
func (heap *MinHeap) GetNode(index int) HeapNode {
	if index < 0 || index >= heap.cap {
		panic("index out of range")
	}
	return heap.nodes[index]
}

// Init function initializes MinHeap, sets up its cap and nodes
// We fill nodes with url "" and count 0, which is "null" node, and we
// can tell it by its count
func (heap *MinHeap) Init(cap int) {
	heap.cap = cap
	heap.nodes = make([]HeapNode, cap)
	for i := 0; i < cap; i++ {
		heap.nodes[i].url = ""
		heap.nodes[i].count = 0
	}
}

// Insert function can insert new node
// if new node's count is less then 0, just do nothing and return
func (heap *MinHeap) Insert(newNode *HeapNode) {
	//newNode.Printf()
	var nowIndex int

	cnt := newNode.count
	if cnt <= 0 {
		return
	}

	if cnt <= heap.nodes[0].count {
		//every node's count >= new node's count
		return
	}
	heap.nodes[0] = *newNode
	for {
		leftIndex := nowIndex*2 + 1
		rightIndex := nowIndex*2 + 2
		chIndex := leftIndex
		if rightIndex < heap.cap {
			if heap.nodes[rightIndex].count < heap.nodes[leftIndex].count {
				chIndex = rightIndex
			}
		}
		if leftIndex >= heap.cap {
			break
		}
		if heap.nodes[chIndex].count < cnt {
			// swap with left child
			heap.nodes[chIndex], heap.nodes[nowIndex] = heap.nodes[nowIndex], heap.nodes[chIndex]
			nowIndex = chIndex
			continue
		}
		break
	}
}

//NewMinHeap function makes a new MinHeap with given cap
func NewMinHeap(cap int) MinHeap {
	var heap MinHeap
	heap.Init(cap)
	return heap
}

/*Debug api which is easy use*/

func (node *HeapNode) Printf() {
	fmt.Printf("%s: %d\n", node.GetURL(), node.GetCount())
}

func (heap *MinHeap) Printf() {
	for i := 0; i < heap.GetCap(); i++ {
		node := heap.GetNode(i)
		node.Printf()
	}
}
