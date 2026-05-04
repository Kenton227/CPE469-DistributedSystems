package main

import (
	"fmt"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"prog4/common"
	"sync"
	"time"
	"strings"
	"regexp"
	"strconv"

	"github.com/PuerkitoBio/goquery"
)

const TIMEOUT_LIMIT = time.Minute
const OUTPUT_DIR = "/app/output"
const START_TIMEOUT = 30 * time.Second
const TEXT_ELEMENTS = "title, h1, h2, h3, h4, h5, h6, p, li, td, th, blockquote, pre, a"

var nonAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

type WorkerRPC struct {
	mutex       sync.Mutex
	// string is workerId, int is reduceIdTask, KeyValue is word -> URL
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
			urls, err := doMapTask(task)
			if err != nil {
				fmt.Println("doMapTask:", err)
				return
			}
			err = reportTaskDone(task, coordClient, urls)
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
			err = reportTaskDone(task, coordClient, nil)
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

func doMapTask(mapTask *common.Task) (map[string]bool, error) {
	fmt.Println("starting map task", mapTask.Id)

	workerState.mutex.Lock()
	defer workerState.mutex.Unlock()

	// go through each url and process the text and links
	urls := make(map[string]bool)
	client := &http.Client{Timeout: 10 * time.Second}
	for _, url := range mapTask.URLs {
		resp, err := client.Get(url)
		if err != nil {
			continue
		}
		doc, err := goquery.NewDocumentFromReader(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		processDocText(doc, mapTask, url)
		processDocLinks(doc, mapTask, url, urls)
	}

	// fmt.Println("mapOutputs:", workerState.mapOutputs)
	// fmt.Println("urls:", urls)

	return urls, nil
}

func processDocText(doc *goquery.Document, mapTask *common.Task, url string) {
	doc.Find("script, style, noscript, svg").Remove()

	doc.Find(TEXT_ELEMENTS).Each(
		func(i int, s *goquery.Selection) {
			text := strings.TrimSpace(s.Text())
			cleanText := nonAlphaNumeric.ReplaceAllString(strings.ToLower(text), " ")
			for _, token := range strings.Fields(cleanText) {
				if token == "" {
					continue
				}
				reduceId := idxHash(text) % mapTask.R
				keyVal := common.KeyValue{Key: token, Value: url}
				updateMapOutput(workerState.addr, reduceId, keyVal)
			}
		},
	)
}

func processDocLinks(doc *goquery.Document, mapTask *common.Task, url string, urls map[string]bool) {
	doc.Find("a[href]").Each(
		func(i int, s *goquery.Selection) {
			link, exists := s.Attr("href")
			if exists {
				link = resolveLink(url, strings.TrimSpace(link))
				if link != "" && isValidHTTP(link) {
					urls[url] = true
				}
			}
		},
	)
}

func isValidHTTP(link string) bool {
	u, err := url.Parse(link)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

/* takes in a baseUrl and a link and returns the absolute path of the link */
func resolveLink(baseUrl string, link string) string {
	parsedBase, err := url.Parse(baseUrl)
	if err != nil {
		return ""
	}
	parsedLink, err := url.Parse(link)
	if err != nil {
		return ""
	}

	return parsedBase.ResolveReference(parsedLink).String()
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

	keyVals, err := fetchIntermediateValues(reduceTask.Id, coordClient)
	if err != nil {
		return err
	}

	fmt.Println(keyVals)

	// make reduce map
	reduceMap := make(map[string]int)
	for _, keyVal := range keyVals {
		val, err := strconv.Atoi(keyVal.Value)
		if err != nil {
			return err
		}
		reduceMap[keyVal.Key] += val
	}

	fmt.Println(reduceMap)
	
	// write output file for this reduce task
	outputFilename := fmt.Sprintf("%s/mr-out-%d.txt", OUTPUT_DIR, reduceTask.Id)

	// make directory and file
	err = os.MkdirAll(OUTPUT_DIR, 0755)
	if err != nil {
		return err
	}
	fptr, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer fptr.Close()

	// write to file
	for word, count := range reduceMap {
		line := fmt.Sprintf("%s: %d\n", word, count)
		_, err = fptr.WriteString(line)
		if err != nil {
			fptr.Close()
			return err
		}
	}

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

func reportTaskDone(task *common.Task, coord *rpc.Client, urls map[string]bool) error {
	args := &common.ReportTaskArgs{
		WorkerAddr: workerState.addr,
		Type:       task.Type,
		TaskID:     task.Id,
		FoundUrls:  urls,
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
			MapOutput: make(map[int][]common.KeyValue),
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
