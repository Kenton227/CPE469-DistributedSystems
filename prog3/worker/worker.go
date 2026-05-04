package main

import (
	"fmt"
	"hash/fnv"
	"net"
	"net/rpc"
	"os"
	"prog3/common"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const TIMEOUT_LIMIT = time.Minute
const OUTPUT_DIR = "/app/output"
const START_TIMEOUT = 30 * time.Second

type WorkerRPC struct {
	mutex sync.Mutex

	// mapOutputs[mapTaskID][reduceID] = []KeyValue
	mapOutputs map[int]map[int][]common.KeyValue
}

var workerState = &WorkerRPC{
	mapOutputs: make(map[int]map[int][]common.KeyValue),
}

func main() {

	workerAddr, err := startWorkerRPCServer()
	if err != nil {
		fmt.Println("startWorkerRPCServer:", err)
		return
	}

	var coordClient *rpc.Client
	startTime := time.Now()
	for time.Since(startTime) < START_TIMEOUT {
		coordClient, err = rpc.Dial("tcp", "coordinator:1234")
		if err == nil {
			fmt.Println("rpc.Dial:", err)
			break
		}
	}

	defer coordClient.Close()

	if err := registerWorker(coordClient, workerAddr); err != nil {
		fmt.Println("registerWorker:", err)
		return
	}

	for {
		if time.Since(startTime) > TIMEOUT_LIMIT {
			fmt.Println("10s connection time out")
			return
		}

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
			err = reportTaskDone(task, workerAddr)
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
			err = reportTaskDone(task, workerAddr)
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

func startWorkerRPCServer() (string, error) {
	if err := rpc.RegisterName("Worker", workerState); err != nil {
		return "", err
	}

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return "", err
	}

	go func() {
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

func doMapTask(mapTask *common.RequestTaskReply) error {
	fmt.Println("starting map task", mapTask.Id)

	data, err := readByteRange(mapTask.Filename, mapTask.StartByte, mapTask.EndByte)
	if err != nil {
		return err
	}

	words := strings.Fields(string(data))

	partitions := make(map[int][]common.KeyValue)
	counts := make([]map[string]int, mapTask.RNum)
	for i := 0; i < mapTask.RNum; i++ {
		counts[i] = make(map[string]int)
	}

	for _, word := range words {
		reduceID := idxHash(word) % mapTask.RNum
		counts[reduceID][word]++
	}

	for reduceID := 0; reduceID < mapTask.RNum; reduceID++ {
		for word, count := range counts[reduceID] {
			partitions[reduceID] = append(partitions[reduceID], common.KeyValue{
				Key:   word,
				Value: strconv.Itoa(count),
			})
		}
	}

	workerState.mutex.Lock()
	workerState.mapOutputs[mapTask.Id] = partitions
	workerState.mutex.Unlock()

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

func doReduceTask(reduceTask *common.RequestTaskReply) error {
	fmt.Println("starting reduce task", reduceTask.Id)

	reduceMap := make(map[string]int)

	type fetchResult struct {
		pairs []common.KeyValue
		err   error
	}

	results := make(chan fetchResult, reduceTask.MNum)

	var wg sync.WaitGroup
	for mapTaskID := 0; mapTaskID < reduceTask.MNum; mapTaskID++ {
		ownerAddr, ok := reduceTask.MapOwners[mapTaskID]
		if !ok {
			return fmt.Errorf("missing owner for map task %d", mapTaskID)
		}

		wg.Add(1)
		go func(mapID int, addr string) {
			defer wg.Done()

			pairs, err := fetchPartition(addr, mapID, reduceTask.Id)
			results <- fetchResult{pairs: pairs, err: err}
		}(mapTaskID, ownerAddr)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		if result.err != nil {
			return result.err
		}
		for _, keyVal := range result.pairs {
			val, err := strconv.Atoi(keyVal.Value)
			if err != nil {
				return err
			}
			reduceMap[keyVal.Key] += val
		}
	}

	err := os.MkdirAll(OUTPUT_DIR, 0755)
	if err != nil {
		return err
	}

	outputFilename := fmt.Sprintf("%s/mr-out-%d.txt", OUTPUT_DIR, reduceTask.Id)
	fptr, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer fptr.Close()

	words := make([]string, 0, len(reduceMap))
	for word := range reduceMap {
		words = append(words, word)
	}
	sort.Strings(words)

	for _, word := range words {
		line := fmt.Sprintf("%s: %d\n", word, reduceMap[word])
		_, err = fptr.WriteString(line)
		if err != nil {
			return err
		}
	}

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

func (w *WorkerRPC) GetPartition(args *common.GetPartitionArgs, reply *common.GetPartitionReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	perMap, ok := w.mapOutputs[args.MapTaskID]
	if !ok {
		reply.Pairs = []common.KeyValue{}
		return nil
	}

	pairs, ok := perMap[args.ReduceID]
	if !ok {
		reply.Pairs = []common.KeyValue{}
		return nil
	}

	reply.Pairs = make([]common.KeyValue, len(pairs))
	copy(reply.Pairs, pairs)
	return nil
}

func requestTask(client *rpc.Client, workerAddr string) (*common.RequestTaskReply, error) {
	args := &common.RequestTaskArgs{WorkerAddr: workerAddr}
	reply := &common.RequestTaskReply{}

	err := client.Call("Coordinator.RequestTask", args, reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func reportTaskDone(task *common.RequestTaskReply, workerAddr string) error {
	client, err := rpc.Dial("tcp", "coordinator:1234")
	if err != nil {
		return err
	}
	defer client.Close()

	args := &common.ReportTaskArgs{
		Type:       task.Type,
		TaskID:     task.Id,
		WorkerAddr: workerAddr,
	}
	reply := &common.ReportTaskReply{}

	return client.Call("Coordinator.ReportTask", args, reply)
}
