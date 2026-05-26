package main

import (
	"database/sql"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"prog7/common"
	"sync"
	"flag"
	"errors"

	_ "github.com/mattn/go-sqlite3"
)

type NodeState int

const (
	StateLeader NodeState = iota
	StateFollower
	StateCandidate
)

type RaftNode struct {
	mutex sync.Mutex
	state NodeState
	term int64
	lastCommittedIdx int64
	lastAppliedIdx int64
}

type Bank struct {
	mutex sync.Mutex
	db    *sql.DB
	raftNode *RaftNode
}

func main() {
	// open database connection
	db, err := sql.Open("sqlite3", "db/bank.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// init RaftNode for Bank struct
	raftNode := &RaftNode{term: 0, state: StateFollower}
	leaderFlag := flag.Bool("L", false, "start as leader")
	flag.Parse()
	if *leaderFlag {
		raftNode.state = StateLeader
	}

	// register Bank struct
	bank := &Bank{db: db, raftNode: raftNode}
	rpc.Register(bank)

	// listen for connections
	ipPort := fmt.Sprintf(":%s", common.BankPort)
	fmt.Println(ipPort)
	listener, err := net.Listen("tcp", ipPort)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer listener.Close()
	fmt.Printf("Bank server is ready and waiting for connections on %s...\n", ipPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("Received connection")
		go rpc.ServeConn(conn)
	}
}

func (bank *Bank) insertLog(entry common.LogEntry) (int64, error) {
	result, err := bank.db.Exec(
		"INSERT INTO operations_log(term, operation, actor_account_id, actor_username, target_account_id, target_username, amount_cents, percentage_bps) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		entry.Term,
		entry.Op,
		entry.ActorAccountID,
		entry.ActorUsername,
		entry.TargetAccountID,
		entry.TargetUsername,
		entry.AmountCents,
		entry.PercentBPS,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (bank *Bank) DoOperation(req common.OperationRequest, reply *common.OperationReply) error {
	logEntry := common.LogEntry{
		Term: bank.raftNode.term,
		Op: req.Op,
		ActorUsername: req.ActorUsername,
		TargetUsername: req.TargetUsername,
		AmountCents: req.AmountCents,
		PercentBPS: req.PercentBPS,
	}

	_, err := bank.insertLog(logEntry)
	if err != nil {
		return err
	}

	reply.OK = true
	return nil
}

func getLogEntry(tx *sql.Tx, logIdx int64) (common.LogEntry, error) {
	var entry common.LogEntry

	var actorAccountID sql.NullInt64
	var targetAccountID sql.NullInt64
	var amountCents sql.NullInt64
	var percentBps sql.NullInt64

	err := tx.QueryRow(`
		SELECT operation, actor_account_id, target_account_id, amount_cents, percentage_bps
		FROM operations_log
		WHERE log_id = ?
		`, logIdx).Scan(
		&entry.Op,
		&actorAccountID,
		&targetAccountID,
		&amountCents,
		&percentBps,
		)
	if err != nil {
		return entry, err
	}
	if actorAccountID.Valid {
		entry.ActorAccountID = actorAccountID.Int64
	}
	if targetAccountID.Valid {
		entry.TargetAccountID = targetAccountID.Int64
	}
	if amountCents.Valid {
		entry.AmountCents = amountCents.Int64
	}
	if percentBps.Valid {
		entry.PercentBPS = percentBps.Int64
	}

	return entry, nil
}

func (bank *Bank) applyLogEntry() (common.OperationReply, error) {
	var reply common.OperationReply

	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	if !(bank.raftNode.lastAppliedIdx < bank.raftNode.lastCommittedIdx) {
		return reply, errors.New("tried to apply uncommitted log index")
	}
	nextLogIdx := bank.raftNode.lastAppliedIdx + 1

	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return reply, err
	}
	defer tx.Rollback()

	entry, err := getLogEntry(tx, nextLogIdx)
	if err != nil {
		return reply, err
	}

	switch entry.Op {
	case common.OpOpen:
		reply, err = openAccount(tx, entry)
	case common.OpClose:
		reply, err = closeAccount(tx, entry)
	case common.OpFreeze:
		reply, err = freezeAccount(tx, entry)
	case common.OpUnfreeze:
		reply, err = unfreezeAccount(tx, entry)
	case common.OpBonus, common.OpInterest:
		reply, err = applyRate(tx, entry)
	case common.OpChargeService:
		reply, err = chargeService(tx, entry)
	case common.OpCheckBal:
		reply, err = checkBalance(tx, entry)
	case common.OpDeposit:
		reply, err = deposit(tx, entry)
	case common.OpWithdraw:
		reply, err = withdraw(tx, entry)
	case common.OpTransfer:
		reply, err = transfer(tx, entry)
	default:
		reply.OK = false
		reply.Message = fmt.Sprintf("unknown operation: %s", entry.Op)
		return reply, nil
	}
	return reply, err
}

func (bank *Bank) applyLogEntries(endLogIdx int64) (common.OperationReply, error) {
	var reply common.OperationReply
	for bank.raftNode.lastAppliedIdx < endLogIdx {
		if reply, err := bank.applyLogEntry(); err != nil {
			return reply, err
		}
	}
	return reply, nil
}
