package main

import (
	"fmt"
	"hash/fnv"
	"net"
	"net/rpc"
	"os"
	"prog4/common"
	"sync"
	"time"
)

const TIMEOUT_LIMIT = time.Minute
const OUTPUT_DIR = "/app/output"
const START_TIMEOUT = 30 * time.Second

type WorkerRPC struct {
	mutex       sync.Mutex
	mapOutputs  map[int][]common.KeyValue
	addr        string
	currentTask *common.Task
}

var workerState = &WorkerRPC{
	mapOutputs: make(map[int][]common.KeyValue),
}

func registerToCoord(workerAddr string) (*rpc.Client, error) {
	coordClient, err := rpc.Dial("tcp", "coordinator:1234")
	if err != nil {
		fmt.Println("rpc.Dial:", err)
		return coordClient, err
	}

	err = registerWorker(coordClient, workerAddr) // Register to coord
	if err != nil {
		fmt.Println("registerWorker:", err)
		return coordClient, err
	}

	fmt.Println("Successful registration to coord")

	return coordClient, err
}

func main() {

	workerAddr, err := startWorkerRPCServer() // Start Worker server
	if err != nil {
		fmt.Println("startWorkerRPCServer:", err)
		return
	}
	workerState.addr = workerAddr

	coordClient, err := registerToCoord(workerAddr) // Register to Coord
	if err != nil {
		fmt.Println("connectCoord: ", err)
		return
	}
	defer coordClient.Close()

	for {
		fmt.Println("Requesting task...")
		task, err := requestTask(coordClient, workerAddr)
		if err != nil {
			fmt.Println("requestTask:", err)
			time.Sleep(time.Second)
			continue
		}

		switch task.Type {
		case common.Map:
			err := doMapTask(task)
			if err != nil {
				fmt.Println("doMapTask:", err)
				return
			}
			err = reportTaskDone(task, coordClient)
			if err != nil {
				fmt.Println("reportTaskDone:", err)
				return
			}

		case common.Reduce:
			err := doReduceTask(task)
			if err != nil {
				fmt.Println("doReduceTask:", err)
				return
			}
			err = reportTaskDone(task, coordClient)
			if err != nil {
				fmt.Println("reportTaskDone:", err)
				return
			}

		case common.Wait:
			fmt.Println("waiting for task...")
			time.Sleep(time.Second)

		case common.Done:
			fmt.Println("nothing to do, exiting...")
			return
		}
	}
}

func (w *WorkerRPC) RecvHeartbeat(args *common.HeartbeatArgs, reply *common.HeartbeatReply) error {
	id := -1
	if w.currentTask != nil {
		id = w.currentTask.Id
	}
	reply.TaskId = id
	reply.WorkerAddr = w.addr
	return nil
}

func startWorkerRPCServer() (string, error) {
	if err := rpc.RegisterName("Worker", workerState); err != nil {
		return "", err
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", err
	}

	go func() { // Threaded listen for requests
		for {
			conn, err := listener.Accept()
			if err != nil {
				fmt.Println("worker accept error:", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	host, err := os.Hostname()
	if err != nil {
		listener.Close()
		return "", err
	}

	// Parse Worker
	port := listener.Addr().(*net.TCPAddr).Port
	addr := fmt.Sprintf("%s:%d", host, port)
	fmt.Println("worker rpc listening at", addr)
	return addr, nil
}

func registerWorker(client *rpc.Client, workerAddr string) error {
	args := &common.RegisterWorkerArgs{WorkerAddr: workerAddr}
	reply := &common.RegisterWorkerReply{}
	return client.Call("Coordinator.RegisterWorker", args, reply)
}

func idxHash(word string) int {
	hash32 := fnv.New32a()
	hash32.Write([]byte(word))
	posHash := int(hash32.Sum32() & 0x7fffffff)
	return posHash
}

func doMapTask(mapTask *common.Task) error {
	fmt.Println("starting map task", mapTask.Id)

	workerState.mutex.Lock()
	defer workerState.mutex.Unlock()

	// TODO: WRITE MAPPING LOGIC...

	// Crawl set of urls in Task.URLs

	// Store key value pairs in workerState.mapOutputs

	return nil
}

func readByteRange(filename string, start int, end int) ([]byte, error) {
	if end < start {
		end = start
	}

	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	size := end - start
	buf := make([]byte, size)

	if size == 0 {
		return buf, nil
	}

	_, err = f.ReadAt(buf, int64(start))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func doReduceTask(reduceTask *common.Task) error {
	fmt.Println("starting reduce task", reduceTask.Id)

	// reduceMap := make(map[string]int)

	// type fetchResult struct {
	// 	pairs []common.KeyValue
	// 	err   error
	// }

	// results := make(chan fetchResult, reduceTask.M)

	// var wg sync.WaitGroup
	// for mapTaskID := 0; mapTaskID < reduceTask.M; mapTaskID++ {
	// 	if !ok {
	// 		return fmt.Errorf("missing owner for map task %d", mapTaskID)
	// 	}

	// 	wg.Add(1)
	// 	go func(mapID int, addr string) {
	// 		defer wg.Done()

	// 		pairs, err := fetchPartition(addr, mapID, reduceTask.Id)
	// 		results <- fetchResult{pairs: pairs, err: err}
	// 	}(mapTaskID, ownerAddr)
	// }

	// go func() {
	// 	wg.Wait()
	// 	close(results)
	// }()

	// for result := range results {
	// 	if result.err != nil {
	// 		return result.err
	// 	}
	// 	for _, keyVal := range result.pairs {
	// 		val, err := strconv.Atoi(keyVal.Value)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		reduceMap[keyVal.Key] += val
	// 	}
	// }

	// err := os.MkdirAll(OUTPUT_DIR, 0755)
	// if err != nil {
	// 	return err
	// }

	// outputFilename := fmt.Sprintf("%s/mr-out-%d.txt", OUTPUT_DIR, reduceTask.Id)
	// fptr, err := os.Create(outputFilename)
	// if err != nil {
	// 	return err
	// }
	// defer fptr.Close()

	// words := make([]string, 0, len(reduceMap))
	// for word := range reduceMap {
	// 	words = append(words, word)
	// }
	// sort.Strings(words)

	// for _, word := range words {
	// 	line := fmt.Sprintf("%s: %d\n", word, reduceMap[word])
	// 	_, err = fptr.WriteString(line)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func fetchPartition(workerAddr string, mapTaskID int, reduceID int) ([]common.KeyValue, error) {
	client, err := rpc.Dial("tcp", workerAddr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	args := &common.GetPartitionArgs{
		MapTaskID: mapTaskID,
		ReduceID:  reduceID,
	}
	reply := &common.GetPartitionReply{}

	err = client.Call("Worker.GetPartition", args, reply)
	if err != nil {
		return nil, err
	}

	return reply.Pairs, nil
}

func requestTask(client *rpc.Client, workerAddr string) (*common.Task, error) {
	args := &common.RequestTaskArgs{WorkerAddr: workerAddr}
	reply := &common.Task{}

	err := client.Call("Coordinator.RequestTask", args, reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func reportTaskDone(task *common.Task, coord *rpc.Client) error {
	args := &common.ReportTaskArgs{
		Type:   task.Type,
		TaskID: task.Id,
	}
	reply := &common.ReportTaskReply{}

	return coord.Call("Coordinator.ReportTask", args, reply)
}
