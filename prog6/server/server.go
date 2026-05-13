package main

import (
	"database/sql"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"prog6/common"
	"sync"

	_ "github.com/mattn/go-sqlite3"
)

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

type Status string

const (
	StatusActive Status = "active"
	StatusFrozen Status = "frozen"
)

type Bank struct {
	db    *sql.DB
	mutex sync.Mutex
}

// TODO: make charge service a negative input value, change error results to either return error or set !OK and return special message based on how severe the error is

func main() {
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

	bank := &Bank{db: db}
	rpc.Register(bank)

	ipPort := fmt.Sprintf("localhost:%s", common.BankPort)
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

func (bank *Bank) GetAccountID(req common.GetIDRequest, reply *common.GetIDReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	var accountID int64

	err := bank.db.QueryRow("SELECT account_id FROM accounts WHERE username = ?", req.Username).Scan(&accountID)
	if err == sql.ErrNoRows {
		reply.OK = false
		reply.ErrorMsg = "username not found"
		reply.AccountID = -1
		return nil
	}
	if err != nil {
		return err
	}

	reply.OK = true
	reply.ErrorMsg = ""
	reply.AccountID = accountID
	return nil
}

func (bank *Bank) OpenAccount(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logID, err := insertLog(tx, OpOpen, nil, nil, nil, nil)
	if err != nil {
		return err
	}

	accountResult, err := tx.Exec(
		"INSERT INTO accounts(username, balance_cents, status) VALUES (?, 0, ?)",
		req.Username,
		StatusActive,
	)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	accountID, err := accountResult.LastInsertId()
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		"UPDATE operations_log SET target_account_id = ? WHERE log_id = ?",
		accountID,
		logID,
	)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Opened account %d for %s", accountID, req.Username)
	return nil
}

func (bank *Bank) CloseAccount(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpClose, nil, &req.TargetAccountID, nil, nil)
	if err != nil {
		return err
	}

	deleteResult, err := tx.Exec("DELETE FROM accounts WHERE account_id = ?", req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	rowsAffected, err := deleteResult.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Closed account %d", req.TargetAccountID)
	return nil
}

func (bank *Bank) FreezeAccount(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpFreeze, nil, &req.TargetAccountID, nil, nil)
	if err != nil {
		return err
	}

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE account_id = ?", StatusFrozen, req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Froze account %d", req.TargetAccountID)
	return nil
}

func (bank *Bank) UnfreezeAccount(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpUnfreeze, nil, &req.TargetAccountID, nil, nil)
	if err != nil {
		return err
	}

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE account_id = ?", StatusActive, req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Unfroze account %d", req.TargetAccountID)
	return nil
}

func (bank *Bank) ApplyRate(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	op := OpBonus
	if req.PercentBPS < 0 {
		op = OpInterest
	}

	_, err = insertLog(tx, op, nil, &req.TargetAccountID, nil, &req.PercentBPS)
	if err != nil {
		return err
	}

	balance, _, exists, err := getAccount(tx, req.TargetAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}

	newBalance := balance + (balance*req.PercentBPS)/10000
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "operation would make balance negative"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Applied %d bps; new balance: %d cents", req.PercentBPS, newBalance)
	return nil
}

func (bank *Bank) ChargeService(req common.TellerRequest, reply *common.TellerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpChargeService, nil, &req.TargetAccountID, &req.AmountCents, nil)
	if err != nil {
		return err
	}

	balance, _, exists, err := getAccount(tx, req.TargetAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}
	if req.AmountCents > 0 {
		reply.OK = false
		reply.Message = "service fee can not be positive"
		return nil
	}

	newBalance := balance + req.AmountCents
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Charged %d cents; new balance: %d cents", req.AmountCents, newBalance)
	return nil
}

func (bank *Bank) CheckBalance(req common.CustomerRequest, reply *common.CustomerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpCheckBal, &req.ActorAccountID, &req.ActorAccountID, nil, nil)
	if err != nil {
		return err
	}

	balance, status, exists, err := getAccount(tx, req.ActorAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}
	if status == string(StatusFrozen) {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Current balance: %d cents", balance)
	return nil
}

func (bank *Bank) Deposit(req common.CustomerRequest, reply *common.CustomerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpDeposit, &req.ActorAccountID, &req.ActorAccountID, &req.AmountCents, nil)
	if err != nil {
		return err
	}

	balance, status, exists, err := getAccount(tx, req.ActorAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}
	if status == string(StatusFrozen) {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}
	if req.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "deposit amount must be positive"
		return nil
	}

	newBalance := balance + req.AmountCents
	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, req.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Deposited %d cents; new balance: %d cents", req.AmountCents, newBalance)
	return nil
}

func (bank *Bank) Withdraw(req common.CustomerRequest, reply *common.CustomerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpWithdraw, &req.ActorAccountID, &req.ActorAccountID, &req.AmountCents, nil)
	if err != nil {
		return err
	}

	balance, status, exists, err := getAccount(tx, req.ActorAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "account not found"
		return nil
	}
	if status == string(StatusFrozen) {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}
	if req.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "withdraw amount must be positive"
		return nil
	}

	newBalance := balance - req.AmountCents
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, req.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Withdrew %d cents; new balance: %d cents", req.AmountCents, newBalance)
	return nil
}

func (bank *Bank) Transfer(req common.CustomerRequest, reply *common.CustomerReply) error {
	bank.mutex.Lock()
	defer bank.mutex.Unlock()

	tx, err := bank.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = insertLog(tx, OpTransfer, &req.ActorAccountID, &req.TargetAccountID, &req.AmountCents, nil)
	if err != nil {
		return err
	}

	if req.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "transfer amount must be positive"
		return nil
	}
	if req.ActorAccountID == req.TargetAccountID {
		reply.OK = false
		reply.Message = "cannot transfer to same account"
		return nil
	}

	actorBalance, actorStatus, exists, err := getAccount(tx, req.ActorAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "actor account not found"
		return nil
	}
	if actorStatus == string(StatusFrozen) {
		reply.OK = false
		reply.Message = "actor account is frozen"
		return nil
	}

	targetBalance, targetStatus, exists, err := getAccount(tx, req.TargetAccountID)
	if err != nil {
		return err
	}
	if !exists {
		reply.OK = false
		reply.Message = "target account not found"
		return nil
	}
	if targetStatus == string(StatusFrozen) {
		reply.OK = false
		reply.Message = "target account is frozen"
		return nil
	}

	if actorBalance < req.AmountCents {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", actorBalance-req.AmountCents, req.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", targetBalance+req.AmountCents, req.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Transferred %d cents to account %d", req.AmountCents, req.TargetAccountID)
	return nil
}

func insertLog(tx *sql.Tx, op Operation, actorID *int64, targetID *int64, amount *int64, percentageBPS *int64) (int64, error) {
	result, err := tx.Exec(
		"INSERT INTO operations_log(operation, actor_account_id, target_account_id, amount_cents, percentage_bps) VALUES (?, ?, ?, ?, ?)",
		op,
		actorID,
		targetID,
		amount,
		percentageBPS,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func getAccount(tx *sql.Tx, accountID int64) (int64, string, bool, error) {
	var balance int64
	var status string
	err := tx.QueryRow("SELECT balance_cents, status FROM accounts WHERE account_id = ?", accountID).Scan(&balance, &status)
	if err == sql.ErrNoRows {
		return 0, "", false, nil
	}
	if err != nil {
		return 0, "", false, err
	}
	return balance, status, true, nil
}
