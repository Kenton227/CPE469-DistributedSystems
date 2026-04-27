package common

import "time"

const BATCH_SIZE = 100

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
	Type   TaskType
	TaskID int
}

type ReportTaskReply struct{}

type RegisterWorkerArgs struct {
	WorkerAddr string
}

type RegisterWorkerReply struct{}

type GetPartitionArgs struct {
	MapTaskID int
	ReduceID  int
}

type GetPartitionReply struct {
	Pairs []KeyValue
}
