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
	mapOutputs  map[string]map[int][]common.KeyValue
	addr        string
	currentTask *common.Task
}

var workerState = &WorkerRPC{
	mapOutputs: make(map[string]map[int][]common.KeyValue),
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
			err := doReduceTask(task, coordClient)
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

	updateMapOutput(workerState.addr, 0, common.KeyValue{Key: "apple", Value: "apple.com"})
	updateMapOutput(workerState.addr, 1, common.KeyValue{Key: "banana", Value: "apple.com"})
	updateMapOutput(workerState.addr, 2, common.KeyValue{Key: "cod", Value: "apple.com"})
	updateMapOutput(workerState.addr, 3, common.KeyValue{Key: "decadent", Value: "apple.com"})
	return nil
}

func updateMapOutput(workerAddr string, reduceId int, intermediatePair common.KeyValue) {
	// Init new worker entry if needed
	if _, ok := workerState.mapOutputs[workerState.addr]; !ok {
		workerState.mapOutputs[workerState.addr] = make(map[int][]common.KeyValue)
	}

	workerState.mapOutputs[workerAddr][reduceId] = append(workerState.mapOutputs[workerAddr][reduceId], intermediatePair)
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

func doReduceTask(reduceTask *common.Task, coordClient *rpc.Client) error {
	fmt.Println("starting reduce task", reduceTask.Id)

	_, err := fetchIntermediateValues(reduceTask.Id, coordClient)
	if err != nil {
		return err
	}

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

// Called by reducer to retrieve all worker addresses from coord
func getWorkerAddresses(coord *rpc.Client) []string {
	args := &common.GetWorkerAddressesArgs{
		RequestingWorkerAddr: workerState.addr,
	}
	reply := &common.GetWorkerAddressesReply{}
	coord.Call("Coordinator.GetWorkerAddresses", args, reply)

	return reply.WorkerAddresses
}

/*
Called by the reducer to retrieve associated intermediate values
Iterates through all registered workers and fetches corresponding map-outputs to ReduceTaskID
*/
func fetchIntermediateValues(reduceTaskID int, coord *rpc.Client) ([]common.KeyValue, error) {

	var intermediatePairs []common.KeyValue

	for _, workerAddr := range getWorkerAddresses(coord) {
		fmt.Println("Requesting intermediate data from worker", workerAddr)
		client, err := rpc.Dial("tcp", workerAddr)
		if err != nil {
			return nil, err // TODO: Ask coord for replica data instead
		}
		defer client.Close()

		args := &common.GetIntermediateValuesArgs{
			ReduceTaskID: reduceTaskID,
		}
		reply := &common.GetIntermediateValuesReply{}

		err = client.Call("Worker.getIntermediateValues", args, reply)
		if err != nil {
			return nil, err
		}

		intermediatePairs = append(intermediatePairs, reply.IntermediatePairs...)
	}

	return intermediatePairs, nil
}

func (w *WorkerRPC) getIntermediateValues(
	args common.GetIntermediateValuesArgs,
	reply common.GetIntermediateValuesReply,
) error {
	fmt.Println("Providing data for Reduce Task", args.ReduceTaskID)
	reply.IntermediatePairs = w.mapOutputs[w.addr][args.ReduceTaskID]
	return nil
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
		WorkerAddr: workerState.addr,
		Type:       task.Type,
		TaskID:     task.Id,
	}
	reply := &common.ReportTaskReply{}
	err := coord.Call("Coordinator.ReportTask", args, reply) // Ask coord who to send replicas to
	if err != nil {
		return err
	}

	// Send replicas to those workers
	for _, addr := range reply.ReplicaWorkerAddrs {
		println("Writing replica to", addr)
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			return nil
		}
		defer client.Close()

		args := &common.AcceptReplicaArgs{
			WorkerAddr: workerState.addr,
		}
		for i, pair := range workerState.mapOutputs[workerState.addr] {
			args.MapOutput[i] = pair
		}
		reply := &common.AcceptReplicaReply{}

		err = client.Call("Worker.AcceptReplica", args, reply)
		if err != nil {
			return nil // TODO: Maybe handle more gracefully?
		}
	}

	return nil
}

func (w *WorkerRPC) AcceptReplica(args common.AcceptReplicaArgs, reply common.AcceptReplicaReply) error {
	workerState.mapOutputs[args.WorkerAddr] = args.MapOutput
	return nil
}
