package common

import (
	"hash/fnv"
	"time"
)

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
	HolderAddresses []string
}

type ReplicaDataType int

const (
	IntermediateData ReplicaDataType = iota
	OutputData
)

type AcceptReplicaArgs struct {
	WorkerAddr   string
	ReduceTaskID int
	FinalOutput  map[string][]string
}

type AcceptReplicaReply struct{}

type ReplicateDataArgs struct {
	ReduceTaskID int
}
type ReplicateDataReply struct {
	Data map[string][]string
}

type DeleteFailedWorkerDataArgs struct {
	FailedAddr string
}
type DeleteFailedWorkerDataReply struct{}

type RequestNewReplicaArgs struct {
	Original      string
	FailedReplica string
}

type RequestNewReplicaReply struct {
	NewReplica string
}

type SearchQueryArgs struct {
	Keyword string
}

type SearchQueryReply struct {
	HolderAddr string
}

type WorkerSearchArgs struct {
	Keyword string
}

type WorkerSearchReply struct {
	URLs []string
}

type MapRecomputeArgs struct {
	Task Task
}

type MapRecomputeReply struct{}

type NotifyFailureArgs struct {
	FailedAddr string
}

type NotifyFailureReply struct{}

func IdxHash(word string, R int) int {
	hash32 := fnv.New32a()
	hash32.Write([]byte(word))
	posHash := int(hash32.Sum32() & 0x7fffffff)
	return posHash % R
}
