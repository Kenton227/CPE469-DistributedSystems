package main

import (
	"fmt"
	"os"
	"prog2/coordinator"
	"strconv"
	"time"
)

func main() {
	// get params for coordinator (M, R, inputFile)
	if len(os.Args) != 4 {
		fmt.Println("error: invalid command format:")
		fmt.Println("go run . <intM> <intR> <inputFile>")
		return
	}

	M, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("error converting arg 1 to int: ", err)
		return
	}

	R, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("error converting arg 2 to int: ", err)
		return
	}

	inputFile := os.Args[3]

	coord, err := coordinator.StartCoordinator(M, R, inputFile)
	if err != nil {
		fmt.Println("coordinator.StartCoordinator: ", err)
		return
	}

	for !coord.Done() {
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("map reduce completed")
}
