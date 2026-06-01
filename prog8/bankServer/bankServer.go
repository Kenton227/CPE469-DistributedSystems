package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"net/rpc"
	"os"
	"prog8/common"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const ELECTION_TIMER_MAX_MS = 300
const ELECTION_TIMER_MIN_MS = 150
const HEARTBEAT_TIMER = 50 * time.Millisecond

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
	lastCommittedIdx int64
	lastAppliedIdx   int64
	currentLeader    string
	address          string
	lastHeartbeat    time.Time
	heartbeatChannel chan struct{}
	currentVote      string
}

type Bank struct {
	mutex    sync.Mutex
	db       *sql.DB
	raftNode *RaftNode
}

func getTesterAddr() string {
	return fmt.Sprintf("tester:%s", common.TesterPort)
}

func initRaftMetadata(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS raft_metadata (
			id                  INTEGER PRIMARY KEY CHECK (id = 1),
			term                INTEGER NOT NULL,
			last_committed_idx  INTEGER NOT NULL,
			last_applied_idx    INTEGER NOT NULL,
			current_vote        TEXT
		);
	`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		INSERT OR IGNORE INTO raft_metadata(id, term, last_committed_idx, last_applied_idx)
		VALUES (1, 0, 0, 0)
	`)
	return err
}

func loadRaftMetadata(db *sql.DB) (int64, int64, int64, error) {
	var term int64
	var lastCommittedIdx int64
	var lastAppliedIdx int64

	err := db.QueryRow(`
		SELECT term, last_committed_idx, last_applied_idx
		FROM raft_metadata
		WHERE id = 1
	`).Scan(&term, &lastCommittedIdx, &lastAppliedIdx)
	if err != nil {
		return 0, 0, 0, err
	}
	return term, lastCommittedIdx, lastAppliedIdx, nil
}

func (bank *Bank) updateRaftMetadata() error {
	_, err := bank.db.Exec(`
		UPDATE raft_metadata
		SET term = ?, last_committed_idx = ?, last_applied_idx = ?, current_vote = ?
		WHERE id = 1
	`,
		bank.raftNode.term,
		bank.raftNode.lastCommittedIdx,
		bank.raftNode.lastAppliedIdx,
		bank.raftNode.currentVote,
	)
	return err
}

func listenForConnections() {
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
		// fmt.Println("Received connection")
		go rpc.ServeConn(conn)
	}
}

func main() {

	addrFlag := flag.String("addr", "", "server address/hostname")
	leaderFlag := flag.Bool("L", false, "start as leader") // Read leader from command-line flag
	flag.Parse()
	if *addrFlag == "" {
		fmt.Println("missing required -addr flag")
		os.Exit(1)
	}

	// open database connection
	db, err := sql.Open("sqlite3", "db/bank.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer db.Close()
	if err := db.Ping(); err != nil { // Test database conn
		fmt.Println(err)
		os.Exit(1)
	}

	if err := initRaftMetadata(db); err != nil { // Init raft_metadata table
		fmt.Println(err)
		os.Exit(1)
	}

	term, lastCommittedIdx, lastAppliedIdx, err := loadRaftMetadata(db) // Test-read from raft_metadata
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// init RaftNode for Bank struct
	raftNode := &RaftNode{
		term:             term,
		state:            StateFollower,
		lastCommittedIdx: lastCommittedIdx,
		lastAppliedIdx:   lastAppliedIdx,
		address:          *addrFlag,
		heartbeatChannel: make(chan struct{}, 1),
	}
	isLeader := *leaderFlag
	if isLeader {
		raftNode.state = StateLeader
		raftNode.currentLeader = raftNode.address
	}

	// register Bank struct
	bank := &Bank{db: db, raftNode: raftNode}
	rpc.Register(bank)

	if isLeader {
		go sendHeartbeats(bank)
	} else {
		go awaitHeartbeats(bank)
	}

	// listen for connections
	listenForConnections()

}

func stepDown(bank *Bank, newTerm int64) error {
	/* Caller must hold mutex */
	bank.raftNode.term = newTerm
	bank.raftNode.state = StateFollower
	bank.raftNode.currentVote = ""
	bank.raftNode.currentLeader = ""

	if err := bank.updateRaftMetadata(); err != nil {
		return err
	}
	return nil
}

func (bank *Bank) HandleVoteRequest(
	request common.RequestVoteRequest,
	reply *common.RequestVoteReply,
) error {

	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	reply.VoteGranted = false
	reply.Term = bank.raftNode.term

	if request.Term > bank.raftNode.term { // Step down as candidate
		if err := stepDown(bank, request.Term); err != nil {
			return err
		}

		reply.Term = bank.raftNode.term
	}

	if request.Term < bank.raftNode.term {
		return nil
	}
	if bank.raftNode.currentVote != "" && bank.raftNode.currentVote != request.CandidateAddr {
		return nil
	}

	lastLoggedIdx, err := getLastLoggedIdx(bank.db)
	if err != nil {
		return err
	}
	lastLoggedTerm, err := getLastLoggedTerm(bank.db)
	if err != nil {
		return err
	}

	candidateUpToDate := request.LastLogTerm > lastLoggedTerm ||
		(request.LastLogTerm == lastLoggedTerm && request.LastLogIdx >= lastLoggedIdx)

	if !candidateUpToDate {
		return nil
	}

	bank.raftNode.currentVote = request.CandidateAddr
	if err = bank.updateRaftMetadata(); err != nil {
		return err
	}

	reply.VoteGranted = true
	return nil
}

func beginElection(bank *Bank) {
	// not yet implemented
	fmt.Println("Beginning election...")
	bank.raftNode.mutex.Lock()
	bank.raftNode.state = StateCandidate
	bank.raftNode.mutex.Unlock()

	return
}

func awaitHeartbeats(bank *Bank) {
	bank.raftNode.mutex.Lock()
	isLeader := bank.raftNode.state == StateLeader
	bank.raftNode.mutex.Unlock()
	if isLeader {
		return
	}
	for {
		electionTimeout := time.Duration(
			ELECTION_TIMER_MIN_MS+
				rand.IntN(ELECTION_TIMER_MAX_MS-ELECTION_TIMER_MIN_MS+1),
		) * time.Millisecond

		timer := time.NewTimer(electionTimeout)

		select {
		case <-bank.raftNode.heartbeatChannel:
			if !timer.Stop() {
				<-timer.C
			}
			continue

		case <-timer.C:

			beginElection(bank)
			return
		}
	}
}

func sendHeartbeats(bank *Bank) error {

	ticker := time.NewTicker(HEARTBEAT_TIMER)
	defer ticker.Stop()

	for range ticker.C {

		bank.raftNode.mutex.Lock()

		if bank.raftNode.state != StateLeader {
			bank.raftNode.mutex.Unlock()
			return nil
		}
		currentCommit := bank.raftNode.lastCommittedIdx
		currentTerm := bank.raftNode.term
		leaderAddr := bank.raftNode.address
		bank.raftNode.mutex.Unlock()

		entryIdx, err := getLastLoggedIdx(bank.db)
		if err != nil {
			return err
		}

		prevLogIdx := entryIdx
		prevLogTerm := int64(0)

		if prevLogIdx > 0 {
			prevEntry, err := getLogEntry(bank.db, prevLogIdx)
			if err != nil {
				return err
			}
			prevLogTerm = prevEntry.Term
		}

		request := common.AppendEntriesRequest{
			Term:         currentTerm,
			LeaderAddr:   leaderAddr,
			PrevLogIdx:   prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      []common.LogEntry{}, // heartbeat = no log entries
			LeaderCommit: currentCommit,
		}

		for _, host := range common.SERVERS {
			if host == leaderAddr {
				continue
			}

			ipPort := fmt.Sprintf("%s:%s", host, common.BankPort)

			client, err := rpc.Dial("tcp", ipPort)
			if err != nil {
				continue
			}

			var appendReply common.AppendEntriesReply
			callErr := client.Call("Bank.AppendEntries", request, &appendReply)
			client.Close()

			if callErr != nil {
				continue
			}

			steppedDown := false

			bank.raftNode.mutex.Lock()
			if appendReply.Term > bank.raftNode.term {
				if err := stepDown(bank, appendReply.Term); err != nil {
					bank.raftNode.mutex.Unlock()
					return err
				}
				steppedDown = true
			}
			bank.raftNode.mutex.Unlock()

			if steppedDown {
				go awaitHeartbeats(bank)

			}
			return nil
		}
	}

	return nil
}

func (bank *Bank) insertLog(entry common.LogEntry) error {
	_, err := bank.db.Exec(
		"INSERT INTO operations_log(log_index, term, operation, actor_username, target_username, amount_cents, percentage_bps) VALUES (?, ?, ?, ?, ?, ?, ?)",
		entry.LogIdx,
		entry.Term,
		entry.Op,
		entry.ActorUsername,
		entry.TargetUsername,
		entry.AmountCents,
		entry.PercentBPS,
	)

	return err

}

func getLogEntry(db *sql.DB, logIdx int64) (common.LogEntry, error) {
	var entry common.LogEntry
	err := db.QueryRow(`
		SELECT log_index, term, operation, actor_username, target_username, amount_cents, percentage_bps
		FROM operations_log
		WHERE log_index = ?
		`, logIdx).Scan(
		&entry.LogIdx,
		&entry.Term,
		&entry.Op,
		&entry.ActorUsername,
		&entry.TargetUsername,
		&entry.AmountCents,
		&entry.PercentBPS,
	)
	return entry, err
}

func getAllLogEntries(db *sql.DB) ([]common.LogEntry, error) {
	rows, err := db.Query(`
		SELECT log_index, term, operation, actor_username, target_username, amount_cents, percentage_bps
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
			&entry.ActorUsername,
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

func getLastLoggedIdx(db *sql.DB) (int64, error) {
	var idx sql.NullInt64

	err := db.QueryRow(`
		SELECT MAX(log_index)
		FROM operations_log
	`).Scan(&idx)
	if err != nil {
		return 0, err
	}

	if !idx.Valid {
		return 0, nil
	}

	return idx.Int64, nil
}

func getLastLoggedTerm(db *sql.DB) (int64, error) {
	var term sql.NullInt64

	err := db.QueryRow(`
		SELECT term
		FROM operations_log
		ORDER BY log_index DESC
		LIMIT 1
	`).Scan(&term)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return term.Int64, nil
}

func newLogEntry(bank *Bank, req common.OperationRequest) (common.LogEntry, error) {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	lastLoggedIdx, err := getLastLoggedIdx(bank.db)
	if err != nil {
		return common.LogEntry{}, err
	}

	nextLogIdx := lastLoggedIdx + 1

	logEntry := common.LogEntry{
		LogIdx:         nextLogIdx,
		Term:           bank.raftNode.term,
		Op:             req.Op,
		ActorUsername:  req.ActorUsername,
		TargetUsername: req.TargetUsername,
		AmountCents:    req.AmountCents,
		PercentBPS:     req.PercentBPS,
	}

	return logEntry, nil
}

func (bank *Bank) appendRequest(req common.OperationRequest, reply *common.OperationReply) error {

	logEntry, err := newLogEntry(bank, req)
	if err != nil {
		return err
	}

	bank.mutex.Lock()

	err = bank.insertLog(logEntry)
	if err != nil {
		bank.mutex.Unlock()
		return err
	}
	bank.mutex.Unlock()

	if err := bank.updateRaftMetadata(); err != nil {
		return err
	}
	reply.OK = true
	return nil
}

/*RPC: Handles client request*/
func (bank *Bank) DoOperation(req common.OperationRequest, reply *common.OperationReply) error {
	bank.raftNode.mutex.Lock()
	if bank.raftNode.state != StateLeader {
		leaderAddr := bank.raftNode.currentLeader
		bank.raftNode.mutex.Unlock()

		reply.OK = false
		reply.Message = "not leader"
		reply.LeaderAddr = leaderAddr
		return nil
	}
	bank.raftNode.mutex.Unlock()

	err := bank.appendRequest(req, reply)
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
	currentCommit := bank.raftNode.lastCommittedIdx
	currentTerm := bank.raftNode.term
	leaderAddr := bank.raftNode.address
	bank.raftNode.mutex.Unlock()

	entryIdx, err := getLastLoggedIdx(bank.db)
	if err != nil {
		return err
	}

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
		Term:         currentTerm,
		LeaderAddr:   leaderAddr,
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      []common.LogEntry{entry},
		LeaderCommit: currentCommit,
	}

	acks_received := 1
	allAcks := true
	for _, host := range common.SERVERS {
		if host == bank.raftNode.address {
			continue
		}
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
			acks_received++
		} else {
			allAcks = false
		}
	}

	majority := (len(common.SERVERS) / 2) + 1

	if acks_received >= majority {
		bank.raftNode.mutex.Lock()
		if entry.Term == bank.raftNode.term && entryIdx > bank.raftNode.lastCommittedIdx {
			bank.raftNode.lastCommittedIdx = entryIdx
			if err := bank.updateRaftMetadata(); err != nil {
				bank.raftNode.mutex.Unlock()
				return err
			}
		}
		bank.raftNode.mutex.Unlock()
	}

	if acks_received < majority {
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

func appendConsistencyCheck(bank *Bank, request common.AppendEntriesRequest) (bool, error) {
	/* Returns true if consistency check succeeds */
	if request.PrevLogIdx == 0 {
		return true, nil
	}

	prevEntry, err := getLogEntry(bank.db, request.PrevLogIdx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if prevEntry.Term != request.PrevLogTerm {
		return false, nil
	}
	return true, nil
}

func repairLog(bank *Bank, logIdx int64) error {
	_, err := bank.db.Exec(
		"DELETE FROM operations_log WHERE log_index >= ?",
		logIdx,
	)
	return err
}

func (bank *Bank) AppendEntries(request common.AppendEntriesRequest, reply *common.AppendEntriesReply) error {
	bank.raftNode.mutex.Lock()
	defer bank.raftNode.mutex.Unlock()

	reply.OK = false
	reply.Term = bank.raftNode.term
	lastLoggedIdx, err := getLastLoggedIdx(bank.db)
	if err != nil {
		return err
	}
	reply.AckIdx = lastLoggedIdx

	if request.Term < bank.raftNode.term {
		return nil
	}

	if request.Term > bank.raftNode.term {
		bank.raftNode.term = request.Term
		bank.raftNode.state = StateFollower
	}

	bank.raftNode.currentLeader = request.LeaderAddr
	bank.raftNode.lastHeartbeat = time.Now()

	select {
	case bank.raftNode.heartbeatChannel <- struct{}{}:
	default:
	}

	lastCommittedIdx := bank.raftNode.lastCommittedIdx

	if err := bank.updateRaftMetadata(); err != nil {
		return err
	}

	ok, err := appendConsistencyCheck(bank, request)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	for _, entry := range request.Entries {
		existing, readErr := getLogEntry(bank.db, entry.LogIdx)

		if readErr != nil {
			if !errors.Is(readErr, sql.ErrNoRows) {
				return readErr
			}
		} else {
			if existing.Term == entry.Term {
				continue

			}
			if err := repairLog(bank, entry.LogIdx); err != nil {
				return err
			}
		}

		if err := bank.insertLog(entry); err != nil {
			return err
		}
	}

	newLastLoggedIdx, err := getLastLoggedIdx(bank.db)
	if err != nil {
		return err
	}

	if request.LeaderCommit > lastCommittedIdx {
		bank.raftNode.lastCommittedIdx = min(request.LeaderCommit, newLastLoggedIdx)
	}

	if err := bank.updateRaftMetadata(); err != nil {
		return err
	}

	reply.OK = true
	reply.Term = bank.raftNode.term
	reply.AckIdx = newLastLoggedIdx
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
	if err := bank.updateRaftMetadata(); err != nil {
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
