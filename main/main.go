package main

import (
	"TopUrl/myhash"
	"TopUrl/myheap"
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const (
	//DataSource is the data file's name
	DataSource = string("DataSource")
	//DataResultTo is the data result file's  name
	DataResultTo = string("DataResult.txt")
	//SplitSH is the path of a shell script which split the big source txt
	SplitSH = string("./init.sh")
	//BatchLines is the number of line per batch
	BatchLines = 100000
	//Buckets is the MOD number ,or bucket number
	Buckets = 256
	//PreConrunLim is the limit number of (pre alloc) Coroutine
	PreConrunLim = 64
	//ReduceConrunLim is the limit number of (reduce work) Coroutine
	ReduceConrunLim = 16
	//TopK is the number of top urls
	TopK = 100
)

var (
	fileMutex [Buckets]sync.Mutex
)

// reduceWork function read the bucketno's file and
func reduceWork(bucketno int, allocCh chan int, heaps []myheap.MinHeap, wg *sync.WaitGroup) {
	defer wg.Done()
	id := <-allocCh
	fmt.Printf("Reduce Worker %d start bucket %d\n", id, bucketno)

	//step1 : read tmp file and build hashmap to get (url, count)s
	file, err := os.Open("../tmp/tmp" + strconv.Itoa(bucketno) + ".txt")
	defer file.Close()
	if err != nil {
		fmt.Printf(err.Error())
	}
	buf := bufio.NewReader(file)
	//fmt.Printf("%d step1 yes\n", id)
	urlMapToCount := make(map[string]int)
	for {
		nowURL, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf(err.Error())
			return
		}
		urlMapToCount[string(nowURL)]++
	}

	//step2 : worker build own min-root heap and insert node(url, count) to it
	//fmt.Printf("%d step2 yes\n", id)

	for url, count := range urlMapToCount {
		node := myheap.NewHeapNode(url, count)
		heaps[bucketno].Insert(&node)
	}

	//step3 : worker send the min-root heap (size TopK) to mainReduce
	//fmt.Printf("%d step3 yes\n", id)
	allocCh <- id
	fmt.Printf("Reduce Worker %d have done bucket %d end\n", id, bucketno)
}

// build worker
func mainReduce() {
	//resultCh 's buffer size should not be ReduceConrunLim but Buckets to avoid deadlock
	allocCh := make(chan int, ReduceConrunLim)
	heaps := make([]myheap.MinHeap, Buckets)
	var wg sync.WaitGroup
	for i := 0; i < Buckets; i++ {
		heaps[i].Init(TopK)
	}

	//alloc reduce task
	for i := 0; i < ReduceConrunLim; i++ {
		allocCh <- i
	}
	for i := 0; i < Buckets; i++ {
		wg.Add(1)
		go reduceWork(i, allocCh, heaps, &wg)
	}

	wg.Wait()
	// final reduce
	close(allocCh)
	nowHeap := myheap.NewMinHeap(TopK)
	for i := 0; i < Buckets; i++ {
		minHeap := heaps[i]
		for j := 0; j < minHeap.GetCap(); j++ {
			node := minHeap.GetNode(j)
			nowHeap.Insert(&node)
		}
		fmt.Printf("Bucket %d ok\n", i)
	}

	// output the result
	filename := "./" + DataResultTo
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	defer file.Close()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	for i := 0; i < nowHeap.GetCap(); i++ {
		node := nowHeap.GetNode(i)
		if node.GetCount() == 0 {
			continue
		}
		out := node.GetURL() + " count is: " + strconv.Itoa(node.GetCount())
		file.Write([]byte(out + "\n"))
	}

}

// we read data from DataSource and we invoke the preAlloc to alloc urls to different file
// this may be like the map-phase in mapreduce.
// notice: we use lineBuf as lines' buffer
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

// This function alloc each url to a temporary-file named like tmp_bucketno, bucketno is
// This is like map phase in mapreduce
// We run perAlloc parallel and we should use mutex per file.
// Because we write file in order 0,1,2..., there is no deadlock
func preAlloc(filePath string, allocCh chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	id := <-allocCh
	fmt.Printf("PreAlloc Worker %d start run\n", id)
	var linesPerFile [][]string
	linesPerFile = make([][]string, Buckets)

	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		fmt.Printf(err.Error())
	}

	buf := bufio.NewReader(file)

	for i := 0; i < Buckets; i++ {
		// we assume result in hash function is uniform
		linesPerFile[i] = make([]string, 0, BatchLines/Buckets/2)
	}
	for {
		nowURL, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf(err.Error())
		}
		line := string(nowURL)
		bucketno := (myhash.MyHashFunction(line)%Buckets + Buckets) % Buckets
		// bucketno must not be negative
		linesPerFile[bucketno] = append(linesPerFile[bucketno], line)
		// fmt.Printf("%d\n", len(linesPerFile[bucketno]))
	}

	for i := 0; i < Buckets; i++ {
		if len(linesPerFile[i]) == 0 {
			continue
		}
		fileMutex[i].Lock()
		filename := "../tmp/tmp" + strconv.Itoa(i) + ".txt"
		file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
		if err != nil {
			fmt.Printf(err.Error())
		}
		for _, url := range linesPerFile[i] {
			file.Write([]byte(url + "\n"))
		}
		file.Close()
		fileMutex[i].Unlock()
	}
	fmt.Printf("PreAlloc Worker %d end\n", id)
	allocCh <- id
}

// build some neccessary directory and files
func buildDir() {
	dir := "../tmp"
	//remove old result
	os.Remove(DataResultTo)
	//remove old temporary data
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf(err.Error())
	}

	if err := os.Mkdir(dir, os.ModePerm); err != nil {
		fmt.Printf(err.Error())
		return
	}
	for i := 0; i < Buckets; i++ {
		os.Create(dir + "/tmp" + strconv.Itoa(i) + ".txt")
	}
}

func main() {
	//If we have no input data we can make it
	Generator(4)
	//If the input data is not splited, we can use the splite shell
	cmd := exec.Command("/bin/bash", "-c", SplitSH)
	err := cmd.Run()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	//Now it's time to deal with split files
	timeStart := time.Now()
	buildDir()
	read()
	mainReduce()
	elapsed := time.Since(timeStart)
	fmt.Println(elapsed)

}
