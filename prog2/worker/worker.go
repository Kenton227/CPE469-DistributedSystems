package main

import (
	"fmt"
	"os"
	"strings"
	"time"
	"strconv"
	"encoding/json"
	"net/rpc"
	"prog2/common"
	"hash/fnv"
)

func main() {
	for {
		task, err := requestTask()
		if err != nil {
			fmt.Println("requestTask: ", err)
			return
		}

		switch task.Type {
		case common.Map:
			fmt.Println("map")
			doMapTask(task)
			reportTaskDone(task)
		case common.Reduce:
			fmt.Println("reduce")
			//doReduceTask(task)
			//reportTaskDone(task)
			break
		case common.Wait:
			fmt.Println("wait")
			time.Sleep(time.Second)
		case common.Done:
			fmt.Println("done")
			break
		}
	}
}

func idxHash(word string) int {
	hash32 := fnv.New32a()
	hash32.Write([]byte(word))
	posHash := int(hash32.Sum32() & 0x7fffffff)
	return posHash
}

func doMapTask(mapTask *common.RequestTaskReply) error {
	// read file
	data, err := os.ReadFile(mapTask.Filename)
	if err != nil {
		return err
	}

	// use MkdirAll so it doesnt fail if intermediates/ already exists
	err = os.MkdirAll("intermediates", 0755)
	if err != nil {
		return err
	}
	
	// split data by spaces
	words := strings.Fields(string(data))

	// make a list of maps for each intermediate file
	maps := make([]map[string]int, mapTask.RNum)
	for i := 0; i < mapTask.RNum; i++ {
		maps[i] = make(map[string]int)
	}

	// add words to their respective map
	for _, word := range words {
	    idx := idxHash(word) % mapTask.RNum
	    maps[idx][word]++
	}

	// write intermediate files
	for i := 0; i < mapTask.RNum; i++ {
		filename := fmt.Sprintf("intermediates/intermediate-%d-%d.json", mapTask.Id, i)

		fptr, err := os.Create(filename)
		if err != nil {
			return err
		}

		// create encoder that writes to the intermediate file and append keyValue to it
		enc := json.NewEncoder(fptr)

		for key, val := range maps[i] {
			keyVal := common.KeyValue{key, strconv.Itoa(val)}
			enc.Encode(keyVal)
		}

		fptr.Close()
	}

	return nil
}

func callRequest(args *common.RequestTaskArgs, reply *common.RequestTaskReply) error {
	client, err := rpc.Dial("tcp", "localhost:7777")
	if err != nil {
		fmt.Println("rpc.Dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Coordinator.RequestTask", args, reply)
	if err != nil {
		return err
	}

	return nil
}

func callReport(args *common.ReportTaskArgs, reply *common.ReportTaskReply) error {
	client, err := rpc.Dial("tcp", "localhost:7777")
	if err != nil {
		fmt.Println("rpc.Dial: ", err)
		return err
	}
	defer client.Close()

	err = client.Call("Coordinator.ReportTask", args, reply)
	if err != nil {
		return err
	}

	return nil
}

func requestTask() (*common.RequestTaskReply, error) {
	args := &common.RequestTaskArgs{}
	reply := &common.RequestTaskReply{}

	err := callRequest(args, reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}
func reportTaskDone(task *common.RequestTaskReply) error {
	args := &common.ReportTaskArgs{task.Type, task.Id}
	reply := &common.ReportTaskReply{}

	err := callReport(args, reply)
	if err != nil {
		return err
	}

	return nil
}
