package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"prog3/common"
	"strconv"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second

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
	startByte int
	endByte   int
}

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

	workers  map[string]bool // registered workers
	mapOwner map[int]string  // map task id -> worker addr
}

func validArgs(args []string) (int, int, string) {
	if len(args) < 4 {
		panic("usage: coordinator <M> <R> <inputFile>")
	}

	M, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println(args[1])
		panic("error parsing M value")
	}

	R, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Println(args[2])
		panic("error parsing R value")
	}

	inputFile := args[3]

	absPath, err := filepath.Abs(inputFile)
	if err != nil {
		panic("failed to resolve input file path")
	}

	_, err = os.Stat(absPath)
	if err == nil {
		fmt.Println("File exists")
	} else if errors.Is(err, os.ErrNotExist) {
		panic("File does not exist")
	} else {
		panic("Error checking file")
	}

	return M, R, absPath
}

func main() {
	M, R, inputFile := validArgs(os.Args)

	fmt.Println("Listening")

	coord, err := StartCoordinator(M, R, inputFile)
	if err != nil {
		fmt.Println("coordinator.StartCoordinator: ", err)
		return
	}

	for !coord.Done() {
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("Completed!")
}

func (coord *Coordinator) RegisterWorker(args *common.RegisterWorkerArgs, reply *common.RegisterWorkerReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	coord.workers[args.WorkerAddr] = true
	fmt.Println("registered worker:", args.WorkerAddr)
	return nil
}

func (coord *Coordinator) RequestTask(args *common.RequestTaskArgs, reply *common.RequestTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	now := time.Now()

	if args.WorkerAddr != "" {
		coord.workers[args.WorkerAddr] = true
	}

	if coord.phase == mapPhase {
		allCompleted := true

		for id, mapTask := range coord.mapTasks {
			if mapTask.status == idle || (mapTask.status == inProgress && now.Sub(mapTask.startTime) > taskTimeout) {
				reply.Type = common.Map
				reply.Id = id
				reply.Filename = mapTask.filename
				reply.StartByte = mapTask.startByte
				reply.EndByte = mapTask.endByte
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
				reply.MapOwners = make(map[int]string, len(coord.mapOwner))
				for k, v := range coord.mapOwner {
					reply.MapOwners[k] = v
				}

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

	switch args.Type {
	case common.Map:
		coord.mapTasks[args.TaskID].status = done
		coord.mapOwner[args.TaskID] = args.WorkerAddr
	case common.Reduce:
		coord.reduceTasks[args.TaskID].status = done
	}

	return nil
}

func isSplitBoundary(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r'
}

// create logical byte-range splits only; do not write split files
func makeSplits(splitNum int, inputFile string, coord *Coordinator) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Println("ReadFile:", err)
		return err
	}

	fileSize := len(data)
	startByte := 0

	for i := 0; i < splitNum; i++ {
		for startByte < fileSize && isSplitBoundary(data[startByte]) {
			startByte++
		}

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

		mapTask := task{
			id:        i,
			status:    idle,
			filename:  inputFile,
			startByte: startByte,
			endByte:   endByte,
		}
		coord.mapTasks = append(coord.mapTasks, mapTask)

		startByte = endByte
	}

	return nil
}

func StartCoordinator(M int, R int, inputFile string) (*Coordinator, error) {
	coord := &Coordinator{
		phase:    mapPhase,
		mNum:     M,
		rNum:     R,
		workers:  make(map[string]bool),
		mapOwner: make(map[int]string),
	}

	if err := rpc.RegisterName("Coordinator", coord); err != nil {
		return nil, err
	}

	err := makeSplits(M, inputFile, coord)
	if err != nil {
		return nil, err
	}

	for i := 0; i < R; i++ {
		reduceTask := task{id: i, status: idle}
		coord.reduceTasks = append(coord.reduceTasks, reduceTask)
	}

	go rpcListen(coord)

	return coord, nil
}

func rpcListen(coord *Coordinator) {
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		fmt.Println("net.Listen:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Coordinator is ready and waiting for connections on port 1234")

	for !coord.Done() {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("connection error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (coord *Coordinator) Done() bool {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()
	return coord.phase == completed
}
