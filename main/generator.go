package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
)

//Generator makes source data.If there is no data, we can use Generator().
func Generator(i int) {
	num := [7]int{10, 100, 1000, 50000, 100000, 200000, 500000}
	filename := "../" + DataSource + "/" + DataSource + ".txt"
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)

	defer file.Close()
	if err != nil {
		fmt.Printf(err.Error())
	}
	for j := 0; j < num[i]; j++ {
		url := "https://www.pingcap.com/blog-cn/" + strconv.Itoa(j)
		for k := 0; k < (int)(math.Sqrt(float64(j)))+1; k++ {
			file.Write([]byte(url + "\n"))
		}
		//Why is sqrt? Because it's easy for us to get the top count
	}
}
