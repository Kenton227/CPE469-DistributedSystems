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
	lastLoggedIdx int64
	lastCommittedIdx int64
	lastAppliedIdx int64
}

type Bank struct {
	mutex sync.Mutex
	db    *sql.DB
	raftNode *RaftNode
}

func initRaftMetadata(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS raft_metadata (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			term INTEGER NOT NULL,
			last_logged_idx INTEGER NOT NULL,
			last_committed_idx INTEGER NOT NULL,
			last_applied_idx INTEGER NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		INSERT OR IGNORE INTO raft_metadata(id, term, last_logged_idx, last_committed_idx, last_applied_idx)
		VALUES (1, 0, 0, 0, 0)
	`)
	return err
}

func loadRaftMetadata(db *sql.DB) (int64, int64, int64, int64, error) {
	var term int64
	var lastLoggedIdx int64
	var lastCommittedIdx int64
	var lastAppliedIdx int64

	err := db.QueryRow(`
		SELECT term, last_logged_idx, last_committed_idx, last_applied_idx
		FROM raft_metadata
		WHERE id = 1
	`).Scan(&term, &lastLoggedIdx, &lastCommittedIdx, &lastAppliedIdx)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	return term, lastLoggedIdx, lastCommittedIdx, lastAppliedIdx, nil
}

func (bank *Bank) storeRaftMetadata() error {
	_, err := bank.db.Exec(`
		UPDATE raft_metadata
		SET term = ?, last_logged_idx = ?, last_committed_idx = ?, last_applied_idx = ?
		WHERE id = 1
	`,
		bank.raftNode.term,
		bank.raftNode.lastLoggedIdx,
		bank.raftNode.lastCommittedIdx,
		bank.raftNode.lastAppliedIdx,
	)
	return err
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

	if err := initRaftMetadata(db); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	term, lastLoggedIdx, lastCommittedIdx, lastAppliedIdx, err := loadRaftMetadata(db)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// init RaftNode for Bank struct
	raftNode := &RaftNode{
		term: term,
		state: StateFollower,
		lastLoggedIdx: lastLoggedIdx,
		lastCommittedIdx: lastCommittedIdx,
		lastAppliedIdx: lastAppliedIdx,
	}
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

func (bank *Bank) insertLog(entry common.LogEntry) error {
	_, err := bank.db.Exec(
		"INSERT INTO operations_log(log_index, term, operation, actor_account_id, actor_username, target_account_id, target_username, amount_cents, percentage_bps) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		entry.LogIdx,
		entry.Term,
		entry.Op,
		entry.ActorAccountID,
		entry.ActorUsername,
		entry.TargetAccountID,
		entry.TargetUsername,
		entry.AmountCents,
		entry.PercentBPS,
	)

	return err

}

func replicateLogs() error {
	return nil
}

func (bank *Bank) insertLogFromReq(req common.OperationRequest, reply *common.OperationReply) error {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	nextLogIdx := bank.raftNode.lastLoggedIdx + 1

	logEntry := common.LogEntry{
		LogIdx: nextLogIdx,
		Term: bank.raftNode.term,
		Op: req.Op,
		ActorUsername: req.ActorUsername,
		TargetUsername: req.TargetUsername,
		AmountCents: req.AmountCents,
		PercentBPS: req.PercentBPS,
	}

	bank.mutex.Lock()

	if req.Op != common.OpOpen {
		if req.ActorUsername.Valid {
			actorAccountId, err := bank.getAccountID(req.ActorUsername.String, reply)
			if err != nil || !reply.OK {
				bank.mutex.Unlock()
				return err
			}
			logEntry.ActorAccountID = actorAccountId
		}
		if req.TargetUsername.Valid {
			targetAccountId, err := bank.getAccountID(req.TargetUsername.String, reply)
			if err != nil || !reply.OK {
				bank.mutex.Unlock()
				return err
			}
			logEntry.TargetAccountID = targetAccountId
		}
	}

	err := bank.insertLog(logEntry)
	if err != nil {
		bank.mutex.Unlock()
		return err
	}
	bank.mutex.Unlock()

	bank.raftNode.lastLoggedIdx = nextLogIdx
	if err := bank.storeRaftMetadata(); err != nil {
		return err
	}
	reply.OK = true
	return nil
}

func (bank *Bank) DoOperation(req common.OperationRequest, reply *common.OperationReply) error {
	err := bank.insertLogFromReq(req, reply)
	if err != nil || !reply.OK {
		return err
	}

	// if err := replicateLogs(); err != nil {
	// 	return err
	// }

	bank.raftNode.mutex.Lock()
	bank.raftNode.lastCommittedIdx = bank.raftNode.lastLoggedIdx
	if err := bank.storeRaftMetadata(); err != nil {
		bank.raftNode.mutex.Unlock()
		return err
	}
	bank.raftNode.mutex.Unlock()

	reply.OK = true
	return nil
}

func getLogEntry(tx *sql.Tx, logIdx int64) (common.LogEntry, error) {
	var entry common.LogEntry

	err := tx.QueryRow(`
		SELECT logIdx, term, operation, actor_account_id, actor_username, target_account_id, target_username, amount_cents, percentage_bps
		FROM operations_log
		WHERE log_id = ?
		`, logIdx).Scan(
		&entry.LogIdx,
		&entry.Term,
		&entry.Op,
		&entry.ActorAccountID,
		&entry.ActorUsername,
		&entry.TargetAccountID,
		&entry.TargetUsername,
		&entry.AmountCents,
		&entry.PercentBPS,
		)
	if err != nil {
		return entry, err
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
	nextAppliedIdx := bank.raftNode.lastAppliedIdx + 1

	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return reply, err
	}
	defer tx.Rollback()

	entry, err := getLogEntry(tx, nextAppliedIdx)
	if err != nil {
		return reply, err
	}

	var rpcErr error
	switch entry.Op {
	case common.OpOpen:
		reply, rpcErr = openAccount(tx, entry)
	case common.OpClose:
		reply, rpcErr = closeAccount(tx, entry)
	case common.OpFreeze:
		reply, rpcErr = freezeAccount(tx, entry)
	case common.OpUnfreeze:
		reply, rpcErr = unfreezeAccount(tx, entry)
	case common.OpBonus, common.OpInterest:
		reply, rpcErr = applyRate(tx, entry)
	case common.OpChargeService:
		reply, rpcErr = chargeService(tx, entry)
	case common.OpCheckBal:
		reply, rpcErr = checkBalance(tx, entry)
	case common.OpDeposit:
		reply, rpcErr = deposit(tx, entry)
	case common.OpWithdraw:
		reply, rpcErr = withdraw(tx, entry)
	case common.OpTransfer:
		reply, rpcErr = transfer(tx, entry)
	default:
		reply.OK = false
		reply.Message = fmt.Sprintf("unknown operation: %s", entry.Op)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	bank.raftNode.lastAppliedIdx = nextAppliedIdx
	if err := bank.storeRaftMetadata(); err != nil {
		return reply, err
	}

	return reply, rpcErr
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
