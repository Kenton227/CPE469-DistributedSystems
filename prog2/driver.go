package main

import (
	"os"
	"strconv"
	"fmt"
	"prog2/coordinator"
)

func main() {
	// NOTE: maybe make the driver start the workers as well (or just make it execute `go run coord.go` with params

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

	// call coordinator
	coordinator.StartCoordinator(M, R, inputFile)
}
