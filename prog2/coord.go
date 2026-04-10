package main

import (
	"fmt"
	"sync"
)

const {
	mapPhase Phase = iota
	reducePhase
	completedPhase
}

type coordinator struct {
	mutex sync.Mutex
	phase Phase
	mapTasks []Task
	reduceTasks []Task
}

func startCoordinator(M int, R int, inputFile string) {
	// TODO: remove
	fmt.Println(M, R, inputFile)

	while (phase
}
