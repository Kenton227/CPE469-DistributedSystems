package coordinator

import (
	"fmt"
	"sync"
	"time"
	"os"
	"net"
	"net/rpc"
	"prog2/common"
)

// enum for task status
type status int
const (
	idle status = iota
	inProgress
	done
)

type task struct {
	id int
	status status
	startTime time.Time
	filename string
}

// enum for coordinator phases
type phase int
const (
	mapPhase phase = iota
	reducePhase
	completed
)

type Coordinator struct {
	mutex sync.Mutex
	phase phase
	mapTasks []task
	reduceTasks []task
	rNum int
}

func (coord *Coordinator) RequestTask(args *common.RequestTaskArgs, reply *common.RequestTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()
	fmt.Println("rpc for RequestTask")

	if coord.phase == mapPhase {
		allCompleted := true
		for id, mapTask := range coord.mapTasks {
			if mapTask.status == inProgress {
				allCompleted = false
			} else if mapTask.status == idle {
				reply.Type = common.Map
				reply.Id = id
				reply.Filename = mapTask.filename
				reply.RNum = coord.rNum

				coord.mapTasks[id].status = inProgress
				return nil
			}
		}

		if allCompleted {
			coord.phase = reducePhase
		} else {
			reply.Type = common.Wait
			return nil
		}

	}

	if coord.phase ==  reducePhase {
		reply.Type = common.Reduce
	}

	return nil
}

func (coord *Coordinator) ReportTask(args *common.ReportTaskArgs, reply *common.ReportTaskReply) error {
	fmt.Println("rpc for ReportTask")
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	if args.Type == common.Map {
		coord.mapTasks[args.TaskID].status = done
	} else if args.Type == common.Reduce {
		coord.reduceTasks[args.TaskID].status = done
	}
	return nil
}

func makeSplits(splitNum int, inputFile string, coord *Coordinator) error {
	data, err := os.ReadFile(inputFile)
	if err != nil {
		fmt.Println("ReadFile: ", err)
		return err
	}

	fileSize := len(data)
	splitSize := fileSize / splitNum

	// use MkdirAll so it doesnt fail if splits/ already exists
	err = os.MkdirAll("splits", 0755)
	if err != nil {
		return err
	}

	for i := 0; i < splitNum; i++ {
		startByte := splitSize * i
		endByte := startByte + splitSize

		// go till end if last split
		if i == splitNum - 1 {
			endByte = fileSize
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
	}

	return nil
}

func StartCoordinator(M int, R int, inputFile string) {
	// initialize coordinator
	coord := &Coordinator{}
	rpc.Register(coord)
	coord.phase = mapPhase
	coord.rNum = R

	// write splits to files at `splits/split-i`
	err := makeSplits(M, inputFile, coord)
	if err != nil {
		fmt.Println("makeSplits: ", err)
		return
	}

	err = rpcListen(coord)
	if err != nil {
		fmt.Println("rpcListen: ", err)
		return
	}
}

func rpcListen(coord *Coordinator) error {
	// open port to listen on
	listener, err := net.Listen("tcp", "localhost:7777")
	if err != nil {
		return err
	}
	fmt.Println("Coordinator is ready and waiting for connections on port 7777")

	// listen loop
	for coord.phase != completed {
		fmt.Println("coord: ", coord)
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("connection error: ", err)
			continue
		}

		// create thread to serve request
		go rpc.ServeConn(conn)
	}

	return nil
}
