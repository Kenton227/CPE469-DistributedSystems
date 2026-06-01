package common

import "database/sql"

const BankPort string = "1234"
const TesterPort string = "1235"

var SERVERS = [...]string{
	"bankserver1",
	"bankserver2",
	"bankserver3",
}

type Operation string

const (
	OpCheckBal      Operation = "check_balance"
	OpDeposit       Operation = "deposit"
	OpWithdraw      Operation = "withdraw"
	OpTransfer      Operation = "transfer"
	OpBonus         Operation = "bonus"
	OpInterest      Operation = "interest"
	OpOpen          Operation = "open"
	OpClose         Operation = "close"
	OpFreeze        Operation = "freeze"
	OpUnfreeze      Operation = "unfreeze"
	OpChargeService Operation = "charge_service"
)

type LogEntry struct {
	LogIdx         int64
	Term           int64
	Op             Operation
	ActorUsername  string
	TargetUsername sql.NullString
	AmountCents    sql.NullInt64
	PercentBPS     sql.NullInt64
}

type OperationRequest struct {
	RequestID      int64
	Op             Operation
	ActorUsername  string
	TargetUsername sql.NullString
	AmountCents    sql.NullInt64
	PercentBPS     sql.NullInt64
}

type OperationReply struct {
	OK         bool
	Message    string
	LeaderAddr string
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     string
	PrevLogIdx   int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	OK     bool
	Term   int64
	AckIdx int64
}

type EmptyRequest struct{}

type CompareLogsReply struct {
	OK      bool
	Message string
}
