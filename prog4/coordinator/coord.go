package main

import (
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"net/url"
	"os"
	"path/filepath"
	"prog4/common"
	"strconv"
	"sync"
	"time"
)

const TASK_TIMEOUT = 10 * time.Second
const HEARTBEAT_INTERVAL = 10 * time.Second
const MAX_URLS = 100
const START_DELAY = 100 * time.Millisecond

type phase int

const (
	mapPhase phase = iota
	reducePhase
	completed
)

type Frontier struct {
	toVisit []string
	known   map[string]bool
}

type Coordinator struct {
	mutex       sync.Mutex
	phase       phase
	frontier    Frontier
	mapTasks    []common.Task
	reduceTasks []common.Task
	mNum        int
	rNum        int
	startTime   time.Time

	workers     map[string]*rpc.Client // registered workers
	mapReplicas map[string][]string
}

func waitTask() *common.Task {
	return &common.Task{
		Type:      common.Wait,
		Id:        -1,
		URLs:      nil,
		StartTime: time.Now(),
		Status:    common.Completed,
	}
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

	go rpcListen(coord)

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

	// TODO: Add replica replication logic on heartbeat failure

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

func advancePhase(coord *Coordinator) {
	var tasks []common.Task
	switch coord.phase {
	case completed:
		return
	case mapPhase:
		if len(coord.frontier.toVisit) > 0 {
			return
		}
		tasks = coord.mapTasks
	case reducePhase:
		tasks = coord.reduceTasks
	}
	allCompleted := true
	for _, task := range tasks {
		if task.Status != common.Completed {
			allCompleted = false
			// fmt.Println("Incomplete task found:", task.Id)
		}
	}
	if allCompleted {
		coord.phase += 1
		fmt.Println("Advanced phase to", coord.phase)
	}
}

// Iteratively scan through coord.reduceTasks for the next
func getReduceTask(coord *Coordinator) *common.Task {
	for _, reduceTask := range coord.reduceTasks {
		if reduceTask.Status == common.Idle || (reduceTask.Status == common.InProgress && time.Since(reduceTask.StartTime) > TASK_TIMEOUT) {
			reduceTask.StartTime = time.Now()
			return &reduceTask
		}
	}
	return nil
}

func getMapTask(coord *Coordinator) *common.Task {

	// Generate new map tasks if frontier is non-empty
	if len(coord.frontier.toVisit) > 0 || len(coord.frontier.known) < MAX_URLS {
		frontierCutoff := min(common.BATCH_SIZE, len(coord.frontier.toVisit))
		newTask := common.Task{
			Type:      common.Map,
			Id:        len(coord.mapTasks),
			URLs:      coord.frontier.toVisit[:frontierCutoff],
			StartTime: time.Now(),
			Status:    common.InProgress,
			R:         coord.rNum,
			M:         coord.mNum,
		}
		coord.frontier.toVisit = coord.frontier.toVisit[frontierCutoff:]
		coord.mapTasks = append(coord.mapTasks, newTask)
		return &newTask
	}

	// Check for any stray map tasks
	for _, task := range coord.mapTasks {
		if task.Status != common.Completed {
			return &task
		}
	}

	// Return wait if no stray tasks or no new tasks to generate
	return waitTask()
}

func (coord *Coordinator) RequestTask(args *common.RequestTaskArgs, reply *common.Task) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	if args.WorkerAddr == "" || coord.workers[args.WorkerAddr] == nil {
		return errors.New("Bad Worker")
	}

	advancePhase(coord)

	var newTask common.Task
	switch coord.phase {
	case mapPhase:
		// fmt.Println("Map task requested")
		newTask = *getMapTask(coord)

	case reducePhase:
		// fmt.Println("Reduce task requested")
		newTask = *getReduceTask(coord)

	case completed:
		newTask = common.Task{
			Type:      common.Done,
			Id:        -1,
			URLs:      nil,
			StartTime: time.Now(),
			Status:    common.Completed,
		}

	default:
		newTask = *waitTask()

	}

	// if time.Since(coord.startTime) < START_DELAY {
	// 	newTask = common.Task{
	// 		Type:      common.Wait,
	// 		Id:        -1,
	// 		URLs:      nil,
	// 		StartTime: time.Now(),
	// 		Status:    common.Completed,
	// 	}
	// }

	*reply = newTask
	return nil
}

// REQUIRES AT LEAST 2 NON-SOURCE WORKERS
func chooseRandomReplicaWorkers(coord *Coordinator, sourceWorker string) (string, string, error) {

	replica_candidates := make([]string, 0)
	for addr, _ := range coord.workers {
		if addr != sourceWorker {
			replica_candidates = append(replica_candidates, addr)
		}
	}

	if len(replica_candidates) < 2 {
		return "", "", errors.New("Not enough workers for replication")
	}

	rand.Shuffle(len(replica_candidates), func(i int, j int) {
		replica_candidates[i], replica_candidates[j] = replica_candidates[j], replica_candidates[i]
	})

	return replica_candidates[0], replica_candidates[1], nil
}

func (coord *Coordinator) ReportTask(args *common.ReportTaskArgs, reply *common.ReportTaskReply) error {
	coord.mutex.Lock()
	defer coord.mutex.Unlock()

	switch args.Type {
	case common.Map:
		if args.TaskID < 0 || args.TaskID >= len(coord.mapTasks) {
			return fmt.Errorf("map task id %d out of range", args.TaskID)
		}
		coord.mapTasks[args.TaskID].Status = common.Completed

	case common.Reduce:
		coord.reduceTasks[args.TaskID].Status = common.Completed
	}

	// Tell workers who to send replicas to
	replica1, replica2, err := chooseRandomReplicaWorkers(coord, args.WorkerAddr)
	if err != nil {
		return nil // TODO: Handle this differently? Right now, we just skip replication entirely
	}
	reply.ReplicaWorkerAddrs = append(reply.ReplicaWorkerAddrs, replica1, replica2)

	// Record replicas
	coord.mapReplicas[args.WorkerAddr] = append(
		coord.mapReplicas[args.WorkerAddr], replica1, replica2,
	)

	return nil
}

func isSplitBoundary(b byte) bool {
	return b == ' ' || b == '\n' || b == '\t' || b == '\r'
}

func isValidHTTP(link string) bool {
	u, err := url.Parse(link)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func loadSeedURLs(coord *Coordinator, inputFile string) error {
	f, err := os.Open(inputFile)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if !isValidHTTP(line) {
			fmt.Println("Invalid seed URL:", line)
			continue
		}
		coord.frontier.toVisit = append(coord.frontier.toVisit, line)
		coord.frontier.known[line] = true
	}
	err = scanner.Err()
	if err != nil {
		return err
	}

	return nil

}

// Initializes the Coordinator Object
func StartCoordinator(M int, R int, inputFile string) (*Coordinator, error) {
	coord := &Coordinator{
		phase:       mapPhase,
		mNum:        M,
		rNum:        R,
		workers:     make(map[string]*rpc.Client),
		mapReplicas: make(map[string][]string),
		frontier:    Frontier{toVisit: make([]string, 0), known: make(map[string]bool)},
		startTime:   time.Now(),
	}

	if err := rpc.RegisterName("Coordinator", coord); err != nil {
		return nil, err
	}

	// Load seed URLS into frontier
	loadSeedURLs(coord, inputFile)

	// Init Reduce Tasks
	for i := 0; i < R; i++ {
		reduceTask := common.Task{
			Type:      common.Reduce,
			Id:        i,
			Status:    common.Idle,
			StartTime: time.Now(),
		}
		coord.reduceTasks = append(coord.reduceTasks, reduceTask)
	}

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

func (coord *Coordinator) GetWorkerAddresses(
	args common.GetWorkerAddressesArgs,
	reply common.GetWorkerAddressesReply,
) error {
	var addresses []string
	for addr, _ := range coord.workers {
		if addr == args.RequestingWorkerAddr {
			continue
		}
		addresses = append(addresses, addr)
	}
	return nil
}
