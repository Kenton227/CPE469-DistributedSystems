package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"prog3/common"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

// enum for task status
type status int

const (
	idle status = iota
	inProgress
	done
)

type task struct {
	id        int
	status    status
	startTime time.Time
	filename  string
}

// enum for coordinator phases
type phase int

const (
	mapPhase phase = iota
	reducePhase
	completed
)

type Coordinator struct {
	mutex       sync.Mutex
	phase       phase
	mapTasks    []task
	reduceTasks []task
	mNum        int
	rNum        int
}

func (coord *Coordinator) RequestTask(args *common.RequestTaskArgs, reply *common.RequestTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	now := time.Now()

	if coord.phase == mapPhase {
		allCompleted := true
		for id, mapTask := range coord.mapTasks {
			if mapTask.status == idle || (mapTask.status == inProgress && now.Sub(mapTask.startTime) > taskTimeout) {
				reply.Type = common.Map
				reply.Id = id
				reply.Filename = mapTask.filename
				reply.RNum = coord.rNum
				reply.MNum = coord.mNum
				coord.mapTasks[id].status = inProgress
				coord.mapTasks[id].startTime = now
				return nil
			}
			if mapTask.status != done {
				allCompleted = false
			}
		}

		if allCompleted {
			coord.phase = reducePhase
		} else {
			reply.Type = common.Wait
			return nil
		}
	}

	if coord.phase == reducePhase {
		allCompleted := true
		for id, reduceTask := range coord.reduceTasks {
			if reduceTask.status == idle || (reduceTask.status == inProgress && now.Sub(reduceTask.startTime) > taskTimeout) {
				reply.Type = common.Reduce
				reply.Id = id
				reply.MNum = coord.mNum
				reply.RNum = coord.rNum
				coord.reduceTasks[id].status = inProgress
				coord.reduceTasks[id].startTime = now
				return nil
			}
			if reduceTask.status != done {
				allCompleted = false
			}
		}

		if allCompleted {
			coord.phase = completed
			reply.Type = common.Done
		} else {
			reply.Type = common.Wait
		}
		return nil
	}

	reply.Type = common.Done
	return nil
}

func (coord *Coordinator) ReportTask(args *common.ReportTaskArgs, reply *common.ReportTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	if args.Type == common.Map {
		coord.mapTasks[args.TaskID].status = done
	} else if args.Type == common.Reduce {
		coord.reduceTasks[args.TaskID].status = done
	}
	return nil
}

func isSplitBoundary(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r'
}

func makeSplits(splitNum int, inputFile string, coord *Coordinator) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Println("ReadFile: ", err)
		return err
	}

	fileSize := len(data)

	// use MkdirAll so it doesnt fail if splits/ already exists
	err = os.MkdirAll("splits", 0755)
	if err != nil {
		return err
	}

	startByte := 0
	for i := 0; i < splitNum; i++ {
		// find a starting byte for this split
		for startByte < fileSize && isSplitBoundary(data[startByte]) {
			startByte++
		}

		// find the ending byte for this split
		endByte := fileSize
		if i != splitNum-1 {
			endByte = ((i + 1) * fileSize) / splitNum
			if endByte < startByte {
				endByte = startByte
			}

			for endByte < fileSize && !isSplitBoundary(data[endByte]) {
				endByte++
			}
		}

		splitData := data[startByte:endByte]

		filename := fmt.Sprintf("splits/split-%d", i)
		err := os.WriteFile(filename, splitData, 0644)
		if err != nil {
			return err
		}

		// add task to coord
		mapTask := task{id: i, status: idle, filename: filename}
		coord.mapTasks = append(coord.mapTasks, mapTask)

		startByte = endByte
	}

	return nil
}

func StartCoordinator(M int, R int, inputFile string) (*Coordinator, error) {
	// initialize coordinator
	coord := &Coordinator{}
	rpc.Register(coord)
	coord.phase = mapPhase
	coord.mNum = M
	coord.rNum = R

	// write splits to files at `splits/split-i` and map tasks
	err := makeSplits(M, inputFile, coord)
	if err != nil {
		return nil, err
	}

	// add reduce tasks
	for i := 0; i < R; i++ {
		reduceTask := task{id: i, status: idle}
		coord.reduceTasks = append(coord.reduceTasks, reduceTask)
	}

	// listen for rpc calls from workers
	go rpcListen(coord)

	return coord, nil
}

func rpcListen(coord *Coordinator) {
	// open port to listen on
	listener, err := net.Listen("tcp", "localhost:7777")
	if err != nil {
		fmt.Println("net.Listen: ", err)
		return
	}
	defer listener.Close()
	fmt.Println("Coordinator is ready and waiting for connections on port 7777")

	// listen loop
	for !coord.Done() {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("connection error: ", err)
			continue
		}

		// create thread to serve request
		go rpc.ServeConn(conn)
	}
}

func (coord *Coordinator) Done() bool {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()
	return coord.phase == completed
}
