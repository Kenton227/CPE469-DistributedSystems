package common

import "time"

const BATCH_SIZE = 10 // 100

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Done
)

type KeyValue struct {
	Key   string
	Value string
}

type taskStatus int

const (
	Idle taskStatus = iota
	InProgress
	Completed
)

type RequestTaskArgs struct {
	WorkerAddr string
}

type Task struct {
	Type      TaskType
	Id        int
	URLs      []string
	StartTime time.Time
	Status    taskStatus
	R         int
	M         int
}

type HeartbeatArgs struct{}
type HeartbeatReply struct {
	WorkerAddr string
	TaskId     int
}

type ReportTaskArgs struct {
	WorkerAddr string
	Type       TaskType
	TaskID     int
	FoundUrls  map[string]bool
}

type ReportTaskReply struct {
	ReplicaWorkerAddrs []string
}

type RegisterWorkerArgs struct {
	WorkerAddr string
}

type RegisterWorkerReply struct{}

type GetIntermediateValuesArgs struct {
	ReduceTaskID int
}

type GetIntermediateValuesReply struct {
	IntermediatePairs []KeyValue
}

type GetWorkerAddressesArgs struct {
	RequestingWorkerAddr string
}

type GetWorkerAddressesReply struct {
	WorkerAddresses []string
}

type AcceptReplicaArgs struct {
	WorkerAddr string
	MapOutput  map[int][]KeyValue
}

type AcceptReplicaReply struct{}
