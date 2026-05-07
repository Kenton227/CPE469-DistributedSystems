package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	fmt.Println("Hello world")

	P := validArgs(os.Args)

	for range P {
		go sendQuery()
	}
}

func validArgs(args []string) int {
	if len(args) < 2 {
		panic("usage: searcher <P>")
	}

	P, err := strconv.Atoi(args[1])
	if err != nil {
		panic("error parsing P value")
	}

	return P
}

func sendQuery() {
	fmt.Println("Hello world")

	// Dial into Coordinator

	//
}
