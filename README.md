# TopUrl

------

TopUrl is my solution to PingCap homework. What I want to talk about is:

> * The problem description
> * My summarization of solution
> * How to use my program
> * The details about my code


## The Problem Description
    Given 100GB file(s), you should find out the 100 urls which have the most frequency.  

## My summarization of solution

### 1. Assumption
To be more strict, I just assume that the file is a big txt file, and every line has a url. And I assume the OS environment is Linux. 
    
### 2. Idea
I think the paper *"MapReduce: Simplified Data Processing on Large Clusters"* is very wonderful. 

Though input data is a big file, we can just split it into many small files. For example we can use shell command to split it by lines, and the number of lines in every small file don't exceed 1000000(some other number is ok).Then we get some split files which satifies the map-reduce's condition in the paper.

We can think the solution as two phase:`map phase` and `reduce phase`. Notice, I don't use distributed method, and I just use map-reduce idea and solve the problem using many `coroutines` to run parallel.

`Map phase` in my program is just to read the small files and alloc and append every url to its temporary file. We use `hash function(MD5)` to determine which file the url should come to. Therefore the same urls will be in the same temporary file. 

`Reduce phase` in my program is to read every temporary file and compute the frequency of every url in the file. I use map in Golang to maintain the (url, frequency). And then I select the Top-100 urls in every temporary file as a result. Finally the results of every temporary file should be merged.

How to select the Top-100 urls in every temporary file? How to merge them? I use `min root heap`, which is easy to maintain the top K elements.

## How to Use My Program
The program language is Golang(1.12.1) and some shell command(Unbuntu), so you may install go.
### Step1
Per Go's [workspace instructions][go-workspace], place TiDB's code on your `GOPATH` using the following cloning procedure

Define a local working directory:
```sh
# If your GOPATH has multiple paths, pick
# just one and use it instead of $GOPATH here.
working_dir=$GOPATH/src
```

Create your clone:

```sh
mkdir -p $working_dir
cd $working_dir
git clone https://github.com/typhigh/TopUrl.git
# the following is recommended
# or: git@github.com:typhigh/TopUrl.git

cd $working_dir/TopUrl
```
### Step2


#### 1. If you have no data or just see how it works, you can do:
```sh
cd main
go build
./main.go > log.txt
# you can also use ./main.go
```
You can see the result in `TopUrl/main/DataResult.txt`, which looks like:
```
https://www.pingcap.com/blog-cn/99911 count is: 317
https://www.pingcap.com/blog-cn/99977 count is: 317
https://www.pingcap.com/blog-cn/99926 count is: 317
https://www.pingcap.com/blog-cn/99863 count is: 317
https://www.pingcap.com/blog-cn/99859 count is: 317
https://www.pingcap.com/blog-cn/99968 count is: 317
https://www.pingcap.com/blog-cn/99907 count is: 317
https://www.pingcap.com/blog-cn/99940 count is: 317
https://www.pingcap.com/blog-cn/99990 count is: 317
https://www.pingcap.com/blog-cn/99900 count is: 317
https://www.pingcap.com/blog-cn/99924 count is: 317
https://www.pingcap.com/blog-cn/99921 count is: 317
https://www.pingcap.com/blog-cn/99901 count is: 317
https://www.pingcap.com/blog-cn/99969 count is: 317
https://www.pingcap.com/blog-cn/99944 count is: 317
...
```
And you can see the log in log.txt:
```
PreAlloc Worker 0 start run
PreAlloc Worker 1 start run
PreAlloc Worker 2 start run
PreAlloc Worker 3 start run
PreAlloc Worker 4 start run
...
PreAlloc Worker 49 end
Reduce Worker 5 start bucket 160
...
Reduce Worker 3 have done bucket 122 end
Reduce Worker 1 have done bucket 121 end
Reduce Worker 11 have done bucket 124 end
Reduce Worker 15 have done bucket 123 end
Bucket 0 ok
Bucket 1 ok
Bucket 2 ok
Bucket 3 ok
...
Bucket 255 ok
42.017945026s
```
The lines
```
PreAlloc Worker 4 start run
...
PreAlloc Worker 49 end
```
says in map phase 4 coroutine starts, and 49 coroutine ends

The lines
```
Reduce Worker 5 start bucket 160
...
Reduce Worker 3 have done bucket 122 end
```
says in reduce phase 5 coroutine starts whose file's number is 160 and 3 coroutine end whose file's number

The lines
```
Bucket 3 ok
```
says the thrid file has been merged 

You can also find `DataSource.txt` file in `TopUrl/DataSource/`.It looks like:
```
https://www.pingcap.com/blog-cn/0
https://www.pingcap.com/blog-cn/1
https://www.pingcap.com/blog-cn/1
https://www.pingcap.com/blog-cn/2
https://www.pingcap.com/blog-cn/2
https://www.pingcap.com/blog-cn/3
https://www.pingcap.com/blog-cn/3
https://www.pingcap.com/blog-cn/4
...
```
This file is made by `generator.go` in `TopUrl/main` . You may see many small files, which is made by `init.sh` in `TopUrl/main/`.

And if you want to test the program, you can just use my unit test.Just do this:
```sh
cd main
go test -v

#the result
...
Bucket 254 ok
Bucket 255 ok
--- PASS: Test_Small (0.56s)
    main_test.go:28: wait right, ok
    main_test.go:71: result is ok
PASS
ok      TopUrl/main     0.582s
```
And do this:
```sh
#assume you are in main
cd ..
cd myheap
go test -v

#the result
=== RUN   Test_NewMinHeap
--- PASS: Test_NewMinHeap (0.00s)
    myheap_test.go:24: NewMinHeap run rigth
=== RUN   Test_Insert
--- PASS: Test_Insert (0.00s)
    myheap_test.go:47: Heap root shows right
    myheap_test.go:70: it's a min root heap, ok
    myheap_test.go:94: heap data is right
PASS
ok      TopUrl/myheap   0.006s
```
#### 2. If you have data
You should name it `DataSource.txt` and move it to `TopUrl/DataSource/`.Then you can comment the line in `main.go`
```
    Generator(4)
```
And then it looks like:
```
func main() {
	//If we have no input data we can make it
//	Generator(4)
	//If the input data is not splited, we can use the splite shell
	cmd := exec.Command("/bin/bash", "-c", SplitSH)
	err := cmd.Run()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	...
```
And then you can build, run and see result in the same way

##The Details about My Code

###Hash Function
I just use MD5 as the hash function, input is a string and output is an 32-bits integer
```
// MyHashFunction is a simple hash function using MD5
func MyHashFunction(s string) int {
	singByte := []byte(s)
	hash := md5.New()
	hash.Write(singByte)
	return bytesToInt(hash.Sum(nil))
}
```

###Gengerator
I use a very simple method to generate the data:
```
func Generator(which int) {
	num := [7]int{10, 100, 1000, 50000, 100000, 200000, 500000}
	filename := "../" + DataSource + "/" + DataSource + ".txt"
	os.Remove(filename)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
    ...
	for j := 0; j < num[which]; j++ {
		url := "https://www.pingcap.com/blog-cn/" + strconv.Itoa(j)
		for k := 0; k < (int)(math.Sqrt(float64(j)))+1; k++ {
			file.Write([]byte(url + "\n"))
		}
		//Why is sqrt? Because it's easy for us to get the top count
	}
}
```
I take `Sqrt + 1` as the url's frequency so we can test the program easily.
And you may want to know the array `num`, the `which` indicates the how big the data file is. When num is 4, the data file is about 0.8GB.

###Min Root Heap

I use the min root heap to maintain Top-K urls. I use (url, count) as a heap node.
```
// HeapNode is the node of MinHeap
type HeapNode struct {
	url   string
	count int
}
```
And the struct of Heap is:
```
// MinHeap is the min root heap, cap may be very small
type MinHeap struct {
	cap   int
	nodes []HeapNode
}
```
`cap` tells the heap's size, and in the problem `cap` is 100.
I use array `nodes` to build heap, which is cache friendly.

The core code in `minheap.go` is:
```
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
```
The current node can just compare its left child and right child. If the node doesn't satisfy the heap's properties, just choose the most big child and swap, go on.

###Main
`main.go` is the core code.
```
    Generator(4)
	//If the input data is not splited, we can use the splite shell
	cmd := exec.Command("/bin/bash", "-c", SplitSH)
	...
	buildDir()
	read()
	mainReduce()
    ...
```

`Generator` makes data, `buildDir` builds neccessary files and directories.`read` is the main map phase, `mainReduce` is the main reduce phase.

```
func read() {

	var wg sync.WaitGroup
	allocCh := make(chan int, PreConrunLim)

	//we put alloc_worker into chan
	for i := 0; i < PreConrunLim; i++ {
		allocCh <- i
	}

	rd, err := ioutil.ReadDir("../" + DataSource)
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	for _, fi := range rd {
		fiName := fi.Name()
		if fiName[len(fiName)-1] != 't' {
			wg.Add(1)
			go preAlloc("../"+DataSource+"/"+fiName, allocCh, &wg)
		}
	}
	wg.Wait()
	close(allocCh)

}
```
`PreConrunLim` indicates how many coroutines should run parallel. `wg` guarantees only after every split files have been read can the program goes on.
The lines:
```
if fiName[len(fiName)-1] != 't'
```
avoids reading the origin file(not split).
And the core in `preAlloc` is:
```
    bucketno := (myhash.MyHashFunction(line)%Buckets + Buckets) % Buckets
    linesPerFile[bucketno] = append(linesPerFile[bucketno], line)
```
and 
```
    fileMutex[i].Lock()
	...
	for _, url := range linesPerFile[i] {
		file.Write([]byte(url + "\n"))
	}
	...
	fileMutex[i].Unlock()
```
I use mutex to lock the shared files. And because in map phase every worker writes file in order 0,1,..., no deadlock will come.

The core in `mainReduce` is:
```
    ...
    for i := 0; i < ReduceConrunLim; i++ {
	    allocCh <- i
    }
	for i := 0; i < Buckets; i++ {
		wg.Add(1)
		go reduceWork(i, allocCh, heaps, &wg)
	}
    ...
	wg.Wait()
	...
```
`wg` is a `barrier` in this code and the program should wait all workers.

And the following code:
```
	for i := 0; i < Buckets; i++ {
		minHeap := heaps[i]
		for j := 0; j < minHeap.GetCap(); j++ {
			node := minHeap.GetNode(j)
			nowHeap.Insert(&node)
		}
		...
	}
```
merge every heap into the final heap.

The core in `reduceWork`:
```
    ...
    for {
		nowURL, _, err := buf.ReadLine()
		...
		urlMapToCount[string(nowURL)]++
	}
	...
```
computes the `count`(`frequency`) of the urls in the file.
And maintains the heap of its own:
```
    ...
    for url, count := range urlMapToCount {
		node := myheap.NewHeapNode(url, count)
		heaps[bucketno].Insert(&node)
	}
	...
```
