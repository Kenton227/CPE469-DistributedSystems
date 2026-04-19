package main

import (
	"fmt"
	"os"
	"strings"
	"time"
	"strconv"
	"errors"
	"io"
	"encoding/json"
	"net/rpc"
	"prog3/common"
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
			err := doMapTask(task)
			if err != nil {
				fmt.Println("doMapTask: ", err)
				return
			}
			err = reportTaskDone(task)
			if err != nil {
				fmt.Println("reportTaskDone: ", err)
				return
			}
		case common.Reduce:
			err := doReduceTask(task)
			if err != nil {
				fmt.Println("doReduceTask: ", err)
				return
			}
			err = reportTaskDone(task)
			if err != nil {
				fmt.Println("reportTaskDone: ", err)
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

func idxHash(word string) int {
	hash32 := fnv.New32a()
	hash32.Write([]byte(word))
	posHash := int(hash32.Sum32() & 0x7fffffff)
	return posHash
}

func doMapTask(mapTask *common.RequestTaskReply) error {
	fmt.Println("starting map task ", mapTask.Id)

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
			err = enc.Encode(keyVal)
			if err != nil {
				fptr.Close()
				return err
			}
		}

		fptr.Close()
	}

	return nil
}

func doReduceTask(reduceTask *common.RequestTaskReply) error {
	fmt.Println("starting reduce task ", reduceTask.Id)

	// read intermediate files for this reduce task
	reduceMap := make(map[string]int)

	for i := 0; i < reduceTask.MNum; i++ {
		filename := fmt.Sprintf("intermediates/intermediate-%d-%d.json", i, reduceTask.Id)

		fptr, err := os.Open(filename)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(fptr)

		// read stream of keyValue pairs and add to reduceMap
		for {
			keyVal := common.KeyValue{}
			err = dec.Decode(&keyVal)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				fptr.Close()
				return err
			}

			val, err := strconv.Atoi(keyVal.Value)
			if err != nil {
				fptr.Close()
				return err
			}
			reduceMap[keyVal.Key] += val
		}

		fptr.Close()
	}

	// write output file for this reduce task
	outputFilename := fmt.Sprintf("mr-outs/mr-out-%d.txt", reduceTask.Id)

	err := os.MkdirAll("mr-outs", 0755)
	if err != nil {
		return err
	}

	fptr, err := os.Create(outputFilename)
	if err != nil {
		return err
	}

	for word, count := range reduceMap {
		line := fmt.Sprintf("%s: %d\n", word, count)
		_, err = fptr.WriteString(line)
		if err != nil {
			fptr.Close()
			return err
		}
	}

	fptr.Close()
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
