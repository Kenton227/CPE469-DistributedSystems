package common

import (
	"time"
)

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
	KnownURLs map[string]bool
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
	OwnerAddr    string
	ReduceTaskID int
}

type GetIntermediateValuesReply struct {
	IntermediatePairs []KeyValue
}

type IntermediateLocation struct {
	OwnerAddr  string
	HolderAddr string
}

type GetIntermediateLocationsArgs struct {
	RequestingWorkerAddr string
}

type GetIntermediateLocationsReply struct {
	Locations []IntermediateLocation
}

type AcceptReplicaArgs struct {
	WorkerAddr string
	MapOutput  map[int][]KeyValue
}

type AcceptReplicaReply struct{}

type ReplicateIntermediateDataArgs struct {
	FailedAddr string
}
type ReplicateIntermediateDataReply struct {
	Data map[int][]KeyValue
}

type DeleteFailedWorkerDataArgs struct {
	FailedAddr string
}
type DeleteFailedWorkerDataReply struct{}
