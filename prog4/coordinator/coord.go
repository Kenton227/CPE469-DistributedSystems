package main

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"prog4/common"
	"strconv"
	"sync"
	"time"
)

const taskTimeout = 10 * time.Second
const HEARTBEAT_INTERVAL = 2 * time.Second

type phase int

const (
	mapPhase phase = iota
	reducePhase
	completed
)

type Coordinator struct {
	mutex       sync.Mutex
	phase       phase
	mapTasks    []common.Task
	reduceTasks []common.Task
	mNum        int
	rNum        int

	workers  map[string]*rpc.Client // registered workers
	mapOwner map[int]string         // map task id -> worker addr
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

func cleanupCoord(coord *Coordinator) {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	for _, client := range coord.workers {
		client.Close()
	}
}

func main() {
	M, R, inputFile := validArgs(os.Args)

	coord, err := StartCoordinator(M, R, inputFile)
	if err != nil {
		fmt.Println("coordinator.StartCoordinator: ", err)
		return
	}

	// err = makeBatches(M, inputFile, coord)
	// if err != nil {
	// 	return
	// }

	lastHeartbeat := time.Now()
	for !coord.Done() {
		if time.Since(lastHeartbeat) > HEARTBEAT_INTERVAL {
			go sendHeartbeats(coord)
			lastHeartbeat = time.Now()
		}
		time.Sleep(500 * time.Millisecond)
	}

	cleanupCoord(coord)
	fmt.Println("Completed!")
}

func sendHeartbeats(coord *Coordinator) error {

	for addr, client := range coord.workers {
		args := &common.HeartbeatArgs{}
		reply := &common.HeartbeatReply{}
		err := client.Call("Worker.RecvHeartbeat", args, reply)
		if err != nil {
			fmt.Println("Heartbeat on", addr, ":", err)
			continue
		}
		fmt.Println(reply.WorkerAddr, " working on ", reply.TaskId)
	}
	return nil
}

func (coord *Coordinator) RegisterWorker(args *common.RegisterWorkerArgs, reply *common.RegisterWorkerReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	client, err := rpc.Dial("tcp", args.WorkerAddr)
	if err != nil {
		fmt.Println("rpc.Dial: ", err)
		return err
	}

	coord.workers[args.WorkerAddr] = client
	return nil
}

func (coord *Coordinator) RequestTask(args *common.RequestTaskArgs, reply *common.Task) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	// now := time.Now()

	// if args.WorkerAddr != "" {
	// 	coord.workers[args.WorkerAddr] = true
	// }

	// if coord.phase == mapPhase {
	// 	allCompleted := true

	// 	for id, mapTask := range coord.mapTasks {
	// 		if mapTask.Status == common.Idle || (mapTask.Status == common.InProgress && now.Sub(mapTask.StartTime) > taskTimeout) {
	// 			reply.Type = common.Map
	// 			reply.Id = id
	// 			reply.Filename = mapTask.Filename

	// 			coord.mapTasks[id].Status = common.InProgress
	// 			coord.mapTasks[id].StartTime = now
	// 			return nil
	// 		}
	// 		if mapTask.Status != common.Completed {
	// 			allCompleted = false
	// 		}
	// 	}

	// 	if allCompleted {
	// 		coord.phase = reducePhase
	// 	} else {
	// 		reply.Type = common.Wait
	// 		return nil
	// 	}
	// }

	// if coord.phase == reducePhase {
	// 	allCompleted := true

	// 	for id, reduceTask := range coord.reduceTasks {
	// 		if reduceTask.Status == common.Idle || (reduceTask.Status == common.InProgress && now.Sub(reduceTask.StartTime) > taskTimeout) {
	// 			reply.Type = common.Reduce
	// 			reply.Id = id
	// 			reply.MapOwners = make(map[int]string, len(coord.mapOwner))
	// 			for k, v := range coord.mapOwner {
	// 				reply.MapOwners[k] = v
	// 			}

	// 			coord.reduceTasks[id].Status = common.InProgress
	// 			coord.reduceTasks[id].StartTime = now
	// 			return nil
	// 		}
	// 		if reduceTask.Status != common.Completed {
	// 			allCompleted = false
	// 		}
	// 	}

	// 	if allCompleted {
	// 		coord.phase = completed
	// 		reply.Type = common.Done
	// 	} else {
	// 		reply.Type = common.Wait
	// 	}
	// 	return nil
	// }

	// reply.Type = common.Done
	return nil
}

func (coord *Coordinator) ReportTask(args *common.ReportTaskArgs, reply *common.ReportTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	switch args.Type {
	case common.Map:
		coord.mapTasks[args.TaskID].Status = common.Completed
		coord.mapOwner[args.TaskID] = args.WorkerAddr
	case common.Reduce:
		coord.reduceTasks[args.TaskID].Status = common.Completed
	}

	return nil
}

func isSplitBoundary(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r'
}

// create m URL batches from urlFile and hold them in coord
func makeBatches(m int, urlFile string, coord *Coordinator) error {
	return nil
}

// Initializes the Coordinator Object
func StartCoordinator(M int, R int, inputFile string) (*Coordinator, error) {
	coord := &Coordinator{
		phase:    mapPhase,
		mNum:     M,
		rNum:     R,
		workers:  make(map[string]*rpc.Client),
		mapOwner: make(map[int]string),
	}

	if err := rpc.RegisterName("Coordinator", coord); err != nil {
		return nil, err
	}

	for i := 0; i < R; i++ {
		reduceTask := common.Task{
			Type:      common.Reduce,
			Id:        i,
			Status:    common.Idle,
			Filename:  "",
			StartTime: time.Now(),
		}
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
