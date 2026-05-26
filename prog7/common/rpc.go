package common

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
	LogId int64
	Term int64
	Op Operation
	ActorAccountID int64
	ActorUsername string
	TargetAccountID int64
	TargetUsername string
	AmountCents int64
	PercentBPS int64
}

type OperationRequest struct {
	Op Operation
	ActorUsername string
	TargetUsername string
	AmountCents int64
	PercentBPS int64
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
