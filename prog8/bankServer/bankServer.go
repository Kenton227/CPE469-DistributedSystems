package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"prog7/common"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

type NodeState int

const (
	StateLeader NodeState = iota
	StateFollower
	StateCandidate
)

type RaftNode struct {
	mutex            sync.Mutex
	state            NodeState
	term             int64
	lastLoggedIdx    int64
	lastCommittedIdx int64
	lastAppliedIdx   int64
}

type Bank struct {
	mutex    sync.Mutex
	db       *sql.DB
	raftNode *RaftNode
}

var followerHosts = []string{"bankserver2", "bankserver3"}

func getTesterAddr() string {
	return fmt.Sprintf("tester:%s", common.TesterPort)
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
		term:             term,
		state:            StateFollower,
		lastLoggedIdx:    lastLoggedIdx,
		lastCommittedIdx: lastCommittedIdx,
		lastAppliedIdx:   lastAppliedIdx,
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

func getLogEntry(db *sql.DB, logIdx int64) (common.LogEntry, error) {
	var entry common.LogEntry
	err := db.QueryRow(`
		SELECT log_index, term, operation, actor_account_id, actor_username, target_account_id, target_username, amount_cents, percentage_bps
		FROM operations_log
		WHERE log_index = ?
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
	return entry, err
}

func getAllLogEntries(db *sql.DB) ([]common.LogEntry, error) {
	rows, err := db.Query(`
		SELECT log_index, term, operation, actor_account_id, actor_username, target_account_id, target_username, amount_cents, percentage_bps
		FROM operations_log
		ORDER BY log_index ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]common.LogEntry, 0)
	for rows.Next() {
		var entry common.LogEntry
		if err := rows.Scan(
			&entry.LogIdx,
			&entry.Term,
			&entry.Op,
			&entry.ActorAccountID,
			&entry.ActorUsername,
			&entry.TargetAccountID,
			&entry.TargetUsername,
			&entry.AmountCents,
			&entry.PercentBPS,
		); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (bank *Bank) insertLogFromReq(req common.OperationRequest, reply *common.OperationReply) error {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	nextLogIdx := bank.raftNode.lastLoggedIdx + 1

	logEntry := common.LogEntry{
		LogIdx:         nextLogIdx,
		Term:           bank.raftNode.term,
		Op:             req.Op,
		ActorUsername:  req.ActorUsername,
		TargetUsername: req.TargetUsername,
		AmountCents:    req.AmountCents,
		PercentBPS:     req.PercentBPS,
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
	bank.raftNode.mutex.Lock()
	if bank.raftNode.state != StateLeader {
		bank.raftNode.mutex.Unlock()
		reply.OK = false
		reply.Message = "not leader"
		return nil
	}
	bank.raftNode.mutex.Unlock()

	err := bank.insertLogFromReq(req, reply)
	if err != nil || !(reply.OK) {
		return err
	}

	if err := bank.replicateLatestEntry(); err != nil {
		return err
	}

	bank.raftNode.mutex.Lock()
	replyLogIdx := bank.raftNode.lastCommittedIdx
	bank.raftNode.mutex.Unlock()

	err = bank.applyLogEntries(replyLogIdx, reply)
	if err != nil {
		return err
	}

	return nil
}

func (bank *Bank) replicateLatestEntry() error {
	bank.raftNode.mutex.Lock()
	entryIdx := bank.raftNode.lastLoggedIdx
	leaderCommit := bank.raftNode.lastCommittedIdx
	term := bank.raftNode.term
	bank.raftNode.mutex.Unlock()

	entry, err := getLogEntry(bank.db, entryIdx)
	if err != nil {
		return err
	}

	prevLogIdx := entryIdx - 1
	prevLogTerm := int64(0)
	if prevLogIdx > 0 {
		prevEntry, err := getLogEntry(bank.db, prevLogIdx)
		if err != nil {
			return err
		}
		prevLogTerm = prevEntry.Term
	}

	request := common.AppendEntriesRequest{
		Term:         term,
		LeaderID:     "bankserver1",
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      []common.LogEntry{entry},
		LeaderCommit: leaderCommit,
	}

	acks := 1
	allAcks := true
	for _, host := range followerHosts {
		ipPort := fmt.Sprintf("%s:%s", host, common.BankPort)
		client, err := rpc.Dial("tcp", ipPort)
		if err != nil {
			allAcks = false
			continue
		}

		var appendReply common.AppendEntriesReply
		callErr := client.Call("Bank.AppendEntries", request, &appendReply)
		client.Close()
		if callErr != nil {
			allAcks = false
			continue
		}
		if appendReply.OK && appendReply.AckIdx >= entryIdx {
			acks++
		} else {
			allAcks = false
		}
	}

	if acks >= 2 {
		bank.raftNode.mutex.Lock()
		if entry.Term == bank.raftNode.term && entryIdx > bank.raftNode.lastCommittedIdx {
			bank.raftNode.lastCommittedIdx = entryIdx
			if err := bank.storeRaftMetadata(); err != nil {
				bank.raftNode.mutex.Unlock()
				return err
			}
		}
		bank.raftNode.mutex.Unlock()
	}

	if acks < 2 {
		return fmt.Errorf("failed to replicate log index %d to majority", entryIdx)
	}

	if allAcks {
		_ = callTesterCompareLogs()
	}

	return nil
}

func callTesterCompareLogs() error {
	client, err := rpc.Dial("tcp", getTesterAddr())
	if err != nil {
		return err
	}
	defer client.Close()

	var reply common.CompareLogsReply
	if err := client.Call("Tester.CompareLogs", common.EmptyRequest{}, &reply); err != nil {
		return err
	}

	if reply.OK {
		fmt.Printf("Tester log compare: OK - %s\n", reply.Message)
	} else {
		fmt.Printf("Tester log compare: MISMATCH - %s\n", reply.Message)
	}

	return nil
}

func (bank *Bank) GetOperationsLog(_ common.EmptyRequest, reply *[]common.LogEntry) error {
	entries, err := getAllLogEntries(bank.db)
	if err != nil {
		return err
	}
	*reply = entries
	return nil
}

func (bank *Bank) AppendEntries(req common.AppendEntriesRequest, reply *common.AppendEntriesReply) error {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	reply.OK = false
	reply.Term = bank.raftNode.term
	reply.AckIdx = bank.raftNode.lastLoggedIdx

	if req.Term < bank.raftNode.term {
		return nil
	}

	if req.Term > bank.raftNode.term {
		bank.raftNode.term = req.Term
		bank.raftNode.state = StateFollower
	}

	if req.PrevLogIdx > 0 {
		prevEntry, err := getLogEntry(bank.db, req.PrevLogIdx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				_ = bank.storeRaftMetadata()
				return nil
			}
			return err
		}
		if prevEntry.Term != req.PrevLogTerm {
			_ = bank.storeRaftMetadata()
			return nil
		}
	}

	for _, entry := range req.Entries {
		existing, err := getLogEntry(bank.db, entry.LogIdx)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		if err == nil {
			if existing.Term != entry.Term {
				_, err = bank.db.Exec("DELETE FROM operations_log WHERE log_index >= ?", entry.LogIdx)
				if err != nil {
					return err
				}
				bank.raftNode.lastLoggedIdx = entry.LogIdx - 1
			} else {
				if entry.LogIdx > bank.raftNode.lastLoggedIdx {
					bank.raftNode.lastLoggedIdx = entry.LogIdx
				}
				continue
			}
		}

		if err := bank.insertLog(entry); err != nil {
			return err
		}
		if entry.LogIdx > bank.raftNode.lastLoggedIdx {
			bank.raftNode.lastLoggedIdx = entry.LogIdx
		}
	}

	if req.LeaderCommit > bank.raftNode.lastCommittedIdx {
		bank.raftNode.lastCommittedIdx = req.LeaderCommit
		bank.raftNode.lastCommittedIdx = min(bank.raftNode.lastCommittedIdx, bank.raftNode.lastLoggedIdx)
	}

	if err := bank.storeRaftMetadata(); err != nil {
		return err
	}

	reply.OK = true
	reply.Term = bank.raftNode.term
	reply.AckIdx = bank.raftNode.lastLoggedIdx
	return nil
}

func (bank *Bank) applyLogEntry(reply *common.OperationReply) error {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	if !(bank.raftNode.lastAppliedIdx < bank.raftNode.lastCommittedIdx) {
		return errors.New("tried to apply uncommitted log index")
	}
	nextAppliedIdx := bank.raftNode.lastAppliedIdx + 1

	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	entry, err := getLogEntry(bank.db, nextAppliedIdx)
	if err != nil {
		return err
	}

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var rpcErr error
	switch entry.Op {
	case common.OpOpen:
		rpcErr = openAccount(tx, entry, reply)
	case common.OpClose:
		rpcErr = closeAccount(tx, entry, reply)
	case common.OpFreeze:
		rpcErr = freezeAccount(tx, entry, reply)
	case common.OpUnfreeze:
		rpcErr = unfreezeAccount(tx, entry, reply)
	case common.OpBonus, common.OpInterest:
		rpcErr = applyRate(tx, entry, reply)
	case common.OpChargeService:
		rpcErr = chargeService(tx, entry, reply)
	case common.OpCheckBal:
		rpcErr = checkBalance(tx, entry, reply)
	case common.OpDeposit:
		rpcErr = deposit(tx, entry, reply)
	case common.OpWithdraw:
		rpcErr = withdraw(tx, entry, reply)
	case common.OpTransfer:
		rpcErr = transfer(tx, entry, reply)
	default:
		reply.OK = false
		reply.Message = fmt.Sprintf("unknown operation: %s", entry.Op)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	bank.raftNode.lastAppliedIdx = nextAppliedIdx
	if err := bank.storeRaftMetadata(); err != nil {
		return err
	}

	return rpcErr
}

func (bank *Bank) applyLogEntries(endLogIdx int64, reply *common.OperationReply) error {
	for bank.raftNode.lastAppliedIdx < endLogIdx {
		if err := bank.applyLogEntry(reply); err != nil {
			return err
		}
	}
	return nil
}
