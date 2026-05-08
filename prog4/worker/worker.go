package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"prog4/common"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const TIMEOUT_LIMIT = time.Minute
const OUTPUT_DIR = "/app/output"
const START_TIMEOUT = 30 * time.Second
const TEXT_ELEMENTS = "title, h1, h2, h3, h4, h5, h6, p, li, td, th, blockquote, pre, a"
const MAX_REDIALS = 10
const INTERMEDIATE_DIR = "/app/intermediate"

var nonAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

type WorkerRPC struct {
	mutex sync.Mutex
	// int is reduceIdTask, KeyValue is word -> URL
	addr          string
	currentTask   *common.Task
	reduceOutputs map[int]map[string][]string
	searchOutputs map[string][]string
	coord         *rpc.Client
}

var workerState = &WorkerRPC{
	reduceOutputs: make(map[int]map[string][]string),
	searchOutputs: make(map[string][]string),
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
	workerState.coord = coordClient

	// Task Request Loop
	for {
		task, err := requestTask(coordClient, workerAddr)
		if err != nil {
			fmt.Println("requestTask:", err)
			time.Sleep(time.Second)
			continue
		}

		workerState.currentTask = task

		switch task.Type {
		case common.Map:
			urls, err := doMapTask(task)
			if err != nil {
				fmt.Println("doMapTask:", err)
				continue
			}
			err = reportTaskDone(task, coordClient, urls)
			if err != nil {
				fmt.Println("reportTaskDone:", err)
				continue
			}

		case common.Reduce:
			err := doReduceTask(task, coordClient)
			if err != nil {
				fmt.Println("doReduceTask:", err)
				continue
			}
			err = reportTaskDone(task, coordClient, nil)
			if err != nil {
				fmt.Println("reportTaskDone:", err)
				continue
			}

		case common.Wait:
			fmt.Println("waiting for task...")
			time.Sleep(time.Second)

			// case common.Done:
			// 	fmt.Println("nothing to do, exiting...")
			// 	return
			// }
		}
		if task.Type == common.Done {
			break
		}
	}

	// Awaiting Search Loop
	for {
		time.Sleep(time.Second)
	}
}

func (w *WorkerRPC) RecvHeartbeat(args *common.HeartbeatArgs, reply *common.HeartbeatReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
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

	host := os.Getenv("WORKER_ADDR")
	if host == "" {
		host, err = os.Hostname()
		if err != nil {
			listener.Close()
			return "", err
		}
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

func doMapTask(mapTask *common.Task) (map[string]bool, error) {
	fmt.Println("starting map task", mapTask.Id)

	urls := make(map[string]bool)
	var urlsMu sync.Mutex
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 10 * time.Second}

	for _, pageURL := range mapTask.URLs {
		resp, err := client.Get(pageURL)
		if err != nil {
			continue
		}

		doc, err := goquery.NewDocumentFromReader(resp.Body)
		resp.Body.Close()
		if err != nil {
			continue
		}

		wg.Add(2)

		go func(doc *goquery.Document, pageURL string) {
			defer wg.Done()
			processDocText(doc, mapTask, pageURL)
		}(doc, pageURL)

		go func(doc *goquery.Document, pageURL string) {
			defer wg.Done()
			processDocLinksSafe(doc, mapTask, pageURL, urls, &urlsMu)
		}(doc, pageURL)
	}

	wg.Wait()
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
				reduceId := common.IdxHash(token, mapTask.R)
				keyVal := common.KeyValue{Key: token, Value: url}
				if err := updateMapOutput(mapTask.Id, reduceId, keyVal); err != nil {
					fmt.Println("updateMapOutput:", err)
				}
			}
		},
	)
}

func processDocLinksSafe(doc *goquery.Document, mapTask *common.Task, pageURL string, urls map[string]bool, mu *sync.Mutex) {
	doc.Find("a[href]").Each(func(i int, s *goquery.Selection) {
		link, exists := s.Attr("href")
		if !exists {
			return
		}

		link = resolveLink(pageURL, strings.TrimSpace(link))
		_, known := mapTask.KnownURLs[link]
		if link != "" && isValidHTTP(link) && !known {
			mu.Lock()
			urls[link] = true
			mu.Unlock()
		}
	})
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

func updateMapOutput(mapTaskId int, reduceId int, intermediatePair common.KeyValue) error {
	workerState.mutex.Lock()
	defer workerState.mutex.Unlock()

	if err := os.MkdirAll(INTERMEDIATE_DIR, 0755); err != nil {
		return err
	}

	filename := fmt.Sprintf("%s/mr-%d-%d.jsonl", INTERMEDIATE_DIR, mapTaskId, reduceId)

	fptr, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer fptr.Close()

	encoder := json.NewEncoder(fptr)
	return encoder.Encode(intermediatePair)
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
		return err // Retry again
	}

	// Make reduce map
	reduceMap := make(map[string]map[string]bool)
	for _, keyVal := range keyVals {
		if _, ok := reduceMap[keyVal.Key]; !ok {
			reduceMap[keyVal.Key] = make(map[string]bool)
		}
		reduceMap[keyVal.Key][keyVal.Value] = true
	}

	// Convert URL sets to slices
	final := make(map[string][]string)
	for word, urlSet := range reduceMap {
		for url := range urlSet {
			final[word] = append(final[word], url)
		}
	}

	workerState.reduceOutputs[reduceTask.Id] = final

	for word, urls := range final {
		workerState.searchOutputs[word] = append([]string{}, urls...)
	}

	if err := os.MkdirAll(OUTPUT_DIR, 0755); err != nil {
		return err
	}

	outputFilename := fmt.Sprintf("%s/mr-out-%d.json", OUTPUT_DIR, reduceTask.Id)

	fptr, err := os.Create(outputFilename)
	if err != nil {
		return err
	}
	defer fptr.Close()

	encoder := json.NewEncoder(fptr)
	encoder.SetIndent("", "  ")

	return encoder.Encode(final)
}

// Called by reducer to retrieve all worker addresses from coord
func getIntermediateDataLocations(coord *rpc.Client) []string {

	args := &common.GetIntermediateLocationsArgs{
		RequestingWorkerAddr: workerState.addr,
	}
	reply := &common.GetIntermediateLocationsReply{}

	err := coord.Call("Coordinator.GetIntermediateLocations", args, reply)
	if err != nil {
		return nil
	}
	return reply.HolderAddresses
}

func handleFailedFetch(failedAddr string) {
	args := &common.NotifyFailureArgs{
		FailedAddr: failedAddr,
	}
	reply := &common.NotifyFailureReply{}
	err := workerState.coord.Call("Coordinator.NotifyFailure", args, reply)
	if err != nil {
		for err != nil {
			err = workerState.coord.Call("Coordinator.NotifyFailure", args, reply)
		}
	}
}

func readIntermediateValues(reduceId int) ([]common.KeyValue, error) {
	var pairs []common.KeyValue

	entries, err := os.ReadDir(INTERMEDIATE_DIR)
	if err != nil {
		if os.IsNotExist(err) {
			return pairs, nil
		}
		return nil, err
	}

	suffix := fmt.Sprintf("-%d.jsonl", reduceId)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, suffix) {
			continue
		}

		filename := fmt.Sprintf("%s/%s", INTERMEDIATE_DIR, name)

		fptr, err := os.Open(filename)
		if err != nil {
			return nil, err
		}

		decoder := json.NewDecoder(fptr)
		for decoder.More() {
			var pair common.KeyValue
			if err := decoder.Decode(&pair); err != nil {
				fptr.Close()
				return nil, err
			}
			pairs = append(pairs, pair)
		}

		fptr.Close()
	}

	return pairs, nil
}

/*
Called by the reducer to retrieve associated intermediate values
Iterates through all registered workers and fetches corresponding map-outputs to ReduceTaskID
*/
func fetchIntermediateValues(reduceTaskID int, coord *rpc.Client) ([]common.KeyValue, error) {

	var intermediatePairs []common.KeyValue

	locations := getIntermediateDataLocations(coord)
	if len(locations) == 0 {
		return nil, errors.New("No data retrieved")
	}
	for _, location := range locations {
		fmt.Println("Requesting intermediate data from", location)
		client, err := rpc.Dial("tcp", location)
		if err != nil {
			handleFailedFetch(location)
			return nil, err
		}
		defer client.Close()

		args := &common.GetIntermediateValuesArgs{
			ReduceTaskID: reduceTaskID,
		}
		reply := &common.GetIntermediateValuesReply{}

		err = client.Call("Worker.GetIntermediateValues", args, reply)
		if err != nil {
			// Handle dead worker
			return nil, err
		}

		intermediatePairs = append(intermediatePairs, reply.IntermediatePairs...)
		// fmt.Println("Fetched", reply.IntermediatePairs, "from", workerAddr)
	}

	return intermediatePairs, nil
}

func (w *WorkerRPC) GetIntermediateValues(
	args *common.GetIntermediateValuesArgs,
	reply *common.GetIntermediateValuesReply,
) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	pairs, err := readIntermediateValues(args.ReduceTaskID)
	if err != nil {
		return err
	}

	reply.IntermediatePairs = pairs
	return nil
}

func (w *WorkerRPC) ReplicateFinalData(
	args *common.ReplicateDataArgs,
	reply *common.ReplicateDataReply,
) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	reply.Data = w.reduceOutputs[args.ReduceTaskID]
	return nil
}

// func (w *WorkerRPC) DeleteFailedWorkerData(
// 	args *common.DeleteFailedWorkerDataArgs,
// 	reply *common.DeleteFailedWorkerDataReply,
// ) error {
// 	w.mutex.Lock()
// 	defer w.mutex.Unlock()
// 	delete(w.mapOutputs, args.FailedAddr)
// 	return nil
// }

func requestTask(client *rpc.Client, workerAddr string) (*common.Task, error) {
	args := &common.RequestTaskArgs{WorkerAddr: workerAddr}
	reply := &common.Task{}

	err := client.Call("Coordinator.RequestTask", args, reply)
	if err != nil {
		return nil, err
	}

	return reply, nil
}

func requestNewReplica(coord *rpc.Client, sourceAddr string, failedReplica string) (string, error) {
	args := &common.RequestNewReplicaArgs{
		Original:      sourceAddr,
		FailedReplica: failedReplica,
	}
	reply := &common.RequestNewReplicaReply{}

	coord.Call("Coordinator.GetNewReplica", args, reply)
	if reply.NewReplica == "" {
		return "", errors.New("No new replica available")
	}
	return reply.NewReplica, nil

}

func reportTaskDone(task *common.Task, coord *rpc.Client, urls map[string]bool) error {
	fmt.Println("Finished with", task.Id)
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
	if task.Type == common.Reduce {

		replicas := append([]string{}, reply.ReplicaWorkerAddrs...)
		// Send intermediate data replicas to assigned workers
		args := &common.AcceptReplicaArgs{
			WorkerAddr:   workerState.addr,
			FinalOutput:  workerState.reduceOutputs[task.Id],
			ReduceTaskID: task.Id,
		}
		replicaReply := &common.AcceptReplicaReply{}

		for i := 0; i < len(replicas); i++ {
			addr := replicas[i]
			fmt.Println("Writing replica to", addr)
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				fmt.Println("Replication failed, trying new worker...")
				newAddr, err := requestNewReplica(coord, workerState.addr, addr)
				if err == nil {
					replicas = append(replicas, newAddr)
				}
				continue // Give up finding new replicas if request fails
			}

			err = client.Call("Worker.AcceptReplica", args, replicaReply)
			client.Close()

			if err != nil {
				fmt.Println("replica write failed:", addr, err)
				continue
			}
		}

	}

	return nil
}

func (w *WorkerRPC) AcceptReplica(args *common.AcceptReplicaArgs, reply *common.AcceptReplicaReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	// Write a version of the final output file
	workerState.reduceOutputs[args.ReduceTaskID] = args.FinalOutput

	for word, urls := range args.FinalOutput {
		workerState.searchOutputs[word] = append([]string{}, urls...)
	}

	return nil
}

func (w *WorkerRPC) HandleSearch(args *common.WorkerSearchArgs, reply *common.WorkerSearchReply) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	keyword := strings.TrimSpace(strings.ToLower(args.Keyword))
	if keyword == "" {
		return errors.New("empty search keyword")
	}

	urls := w.searchOutputs[keyword]
	reply.URLs = append([]string{}, urls...)
	return nil
}

func (w *WorkerRPC) MapRecompute(
	args *common.MapRecomputeArgs,
	reply *common.AcceptReplicaReply,
) error {
	urls, err := doMapTask(&args.Task)
	if err != nil {
		fmt.Println("doMapTask:", err)
		return err
	}
	err = reportTaskDone(&args.Task, w.coord, urls)
	if err != nil {
		fmt.Println("reportTaskDone:", err)
		return err
	}
	return nil
}
