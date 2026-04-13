package common

// enum for taskType
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

type RequestTaskArgs struct {}
type RequestTaskReply struct {
	Type TaskType
	Id int
	Filename string
	RNum int
}

type ReportTaskArgs struct {
	Type TaskType
	TaskID int
}
type ReportTaskReply struct {
}
