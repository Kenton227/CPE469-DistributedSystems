package common

import "database/sql"

const BankPort string = "1234"

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
	LogIdx int64
	Term int64
	Op Operation
	ActorAccountID sql.NullInt64
	ActorUsername sql.NullString
	TargetAccountID sql.NullInt64
	TargetUsername sql.NullString
	AmountCents sql.NullInt64
	PercentBPS sql.NullInt64
}

type OperationRequest struct {
	Op Operation
	ActorUsername sql.NullString
	TargetUsername sql.NullString
	AmountCents sql.NullInt64
	PercentBPS sql.NullInt64
}

type OperationReply struct {
	OK bool
	Message string
}

type AppendEntriesRequest struct {
	Entries []LogEntry
}

type AppendEntriesReply struct {
	OK bool
	Term int64
}
