package myheap

import (
	"strconv"
	"testing"
)

//TestNew tests NewMinHeap and NewHeapNode
func Test_NewMinHeap(t *testing.T) {
	flag := true
	if heap := NewMinHeap(10); heap.GetCap() == 10 {
		for i := 0; i < 10; i++ {
			node := heap.GetNode(i)
			//every new node should be ("", 0)
			if node.GetCount() != 0 || node.GetURL() != string("") {
				flag = false
			}
		}
	} else {
		flag = false
	}

	if flag {
		t.Log("NewMinHeap run rigth")
	} else {
		t.Error("NewMinHeap fails")
	}
}

//TestInsert tests the Insert method on MinHeap
func Test_Insert(t *testing.T) {
	n := 100
	heap := NewMinHeap(n)
	flag := true
	for i := n; i >= 1; i-- {
		node := NewHeapNode("a."+strconv.Itoa(i), i)
		heap.Insert(&node)
		if node == heap.GetNode(0) && i != 1 {
			flag = false
		}
		if i == 1 && node != heap.GetNode(0) {
			flag = false
		}
	}

	if flag {
		t.Log("Heap root shows right")
	} else {
		t.Error("Insert fails")
	}

	flag = true
	for i := 0; 2*i+1 < n; i++ {
		//test: current node's count must not greater than child's
		node := heap.GetNode(i)
		childl := heap.GetNode(2*i + 1)
		if node.GetCount() > childl.GetCount() {
			flag = false
		}
		if 2*i+2 >= n {
			continue
		}
		childr := heap.GetNode(2*i + 2)
		if node.GetCount() > childr.GetCount() {
			flag = false
		}
	}

	if flag {
		t.Log("it's a min root heap, ok")
	} else {
		t.Error("it's not a min root heap, fails")
	}

	flag = true
	vis := make([]bool, n+1)
	//We shouldn't miss any node
	for i := 0; i < n; i++ {
		node := heap.GetNode(i)
		count := node.GetCount()
		vis[count] = true
		expect := "a." + strconv.Itoa(count)
		if expect != node.GetURL() {
			flag = false
		}
	}

	for i := 1; i <= n; i++ {
		if !vis[i] {
			flag = false
		}
	}
	if flag {
		t.Log("heap data is right")
	} else {
		t.Error("heap data is wrong")
	}
}
