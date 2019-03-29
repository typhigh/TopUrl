package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"testing"
)

//Test_Small uses small sample to test correctness
func Test_Small(t *testing.T) {
	buildDir()
	var wg sync.WaitGroup
	files := [2]string{"testa.txt", "testb.txt"}
	allocCh := make(chan int, PreConrunLim)
	for i := 0; i < PreConrunLim; i++ {
		allocCh <- i
	}
	for _, fileName := range files {
		wg.Add(1)
		go preAlloc(fileName, allocCh, &wg)
	}
	wg.Wait()
	close(allocCh)
	t.Log("wait right, ok")

	mainReduce()
	filePath := string(DataResultTo)
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		fmt.Printf(err.Error())
	}

	buf := bufio.NewReader(file)
	lineExist := make(map[string]bool)
	//lineExist is what we expect
	for i := 1; i <= 5; i++ {
		lineExist["test"+strconv.Itoa(i)+" count is: "+strconv.Itoa(i*2)] = true
	}
	flag := true
	for {
		nowURL, _, err := buf.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf(err.Error())
		}
		if lineExist[string(nowURL)] == false {
			//now we find some data wrong
			flag = false
			break
		} else {
			//if (url1, c1) and (url1, c2) exists, we should think it's wrong
			//such as "test1 count is: 1" and "test1 count is: 1"
			lineExist[string(nowURL)] = false
		}
	}
	//make sure every source data we don't miss
	for i := 1; i <= 5; i++ {
		if lineExist["test"+strconv.Itoa(i)+" count is: "+strconv.Itoa(i*2)] == true {
			flag = false
		}
	}

	if flag == true {
		t.Log("result is ok")
	} else {
		t.Error("result is not ok")
	}
}
