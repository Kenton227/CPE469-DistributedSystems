package common

import "time"

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

type RequestTaskArgs struct {
	WorkerAddr string
}

type taskStatus int

const (
	Idle taskStatus = iota
	InProgress
	Completed
)

type Task struct {
	Type      TaskType
	Id        int
	Filename  string
	StartTime time.Time
	Status    taskStatus
	R         int
	M         int

	// for reduce tasks: map task id -> worker addr that owns that map output
	MapOwners map[int]string
}

type HeartbeatArgs struct{}
type HeartbeatReply struct {
	WorkerAddr string
	TaskId     int
}

type ReportTaskArgs struct {
	Type       TaskType
	TaskID     int
	WorkerAddr string
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
