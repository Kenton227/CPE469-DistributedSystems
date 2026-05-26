package main

import (
	"database/sql"
	"fmt"
	"prog7/common"

	_ "github.com/mattn/go-sqlite3"
)

type Status string

const (
	StatusActive Status = "active"
	StatusFrozen Status = "frozen"
)

type Account struct {
	accountId int64
	username string
	balanceCents int64
	status Status
}

func getAccountID(tx *sql.Tx, username string) (int64, error) {
	var accountID int64
	err := tx.QueryRow("SELECT account_id FROM accounts WHERE username = ?", username).Scan(&accountID)
	if err != nil {
		return accountID, err
	}
	return accountID, nil
}

func getAccountEntry(tx *sql.Tx, accountID int64) (Account, error) {
	acc := Account{accountId: accountID}
	err := tx.QueryRow("SELECT username, balance_cents, status FROM accounts WHERE account_id = ?", accountID).Scan(&acc.username, &acc.balanceCents, &acc.status)
	if err != nil {
		return acc, err
	}
	return acc, nil
}

func openAccount(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	accountResult, err := tx.Exec(
		"INSERT INTO accounts(username, balance_cents, status) VALUES (?, 0, ?)",
		entry.TargetUsername,
		StatusActive,
	)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	newAccountID, err := accountResult.LastInsertId()
	if err != nil {
		return reply, err
	}

	_, err = tx.Exec(
		"UPDATE operations_log SET target_account_id = ? WHERE log_id = ?",
		newAccountID,
		entry.LogId,
	)
	if err != nil {
		return reply, err
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Opened account %d for %s", newAccountID, entry.TargetUsername)
	return reply, nil
}

func closeAccount(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	deleteResult, err := tx.Exec("DELETE FROM accounts WHERE account_id = ?", entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	rowsAffected, err := deleteResult.RowsAffected()
	if err != nil {
		return reply, err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Closed account %d", entry.TargetAccountID)
	return reply, nil
}

func freezeAccount(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE account_id = ?", StatusFrozen, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return reply, err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Froze account %d", entry.TargetAccountID)
	return reply, nil
}

func unfreezeAccount(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE account_id = ?", StatusActive, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return reply, err
	}
	if rowsAffected == 0 {
		reply.OK = false
		reply.Message = "account not found"
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Unfroze account %d", entry.TargetAccountID)
	return reply, nil
}

func applyRate(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	targetAccount, err := getAccountEntry(tx, entry.TargetAccountID)
	if err != nil {
		return reply, err
	}

	newBalance := targetAccount.balanceCents + (targetAccount.balanceCents*entry.PercentBPS)/10000
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "operation would make balance negative"
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Applied %d bps; new balance: %d cents", entry.PercentBPS, newBalance)
	return reply, nil
}

func chargeService(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	targetAccount, err := getAccountEntry(tx, entry.TargetAccountID)
	if err != nil {
		return reply, err
	}
	if entry.AmountCents > 0 {
		reply.OK = false
		reply.Message = "service fee can not be positive"
		return reply, nil
	}

	newBalance := targetAccount.balanceCents + entry.AmountCents
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Charged %d cents; new balance: %d cents", entry.AmountCents, newBalance)
	return reply, nil
}

func checkBalance(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	actorAccount, err := getAccountEntry(tx, entry.ActorAccountID)
	if err != nil {
		return reply, err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Current balance: %d cents", actorAccount.balanceCents)
	return reply, nil
}

func deposit(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	actorAccount, err := getAccountEntry(tx, entry.ActorAccountID)
	if err != nil {
		return reply, err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return reply, nil
	}
	if entry.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "deposit amount must be positive"
		return reply, nil
	}

	newBalance := actorAccount.balanceCents + entry.AmountCents
	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, entry.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Deposited %d cents; new balance: %d cents", entry.AmountCents, newBalance)
	return reply, nil
}

func withdraw(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	actorAccount, err := getAccountEntry(tx, entry.ActorAccountID)
	if err != nil {
		return reply, err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return reply, nil
	}
	if entry.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "withdraw amount must be positive"
		return reply, nil
	}

	newBalance := actorAccount.balanceCents - entry.AmountCents
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", newBalance, entry.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Withdrew %d cents; new balance: %d cents", entry.AmountCents, newBalance)
	return reply, nil
}

func transfer(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	if entry.AmountCents <= 0 {
		reply.OK = false
		reply.Message = "transfer amount must be positive"
		return reply, nil
	}
	if entry.ActorAccountID == entry.TargetAccountID {
		reply.OK = false
		reply.Message = "cannot transfer to same account"
		return reply, nil
	}

	actorAccount, err := getAccountEntry(tx, entry.ActorAccountID)
	if err != nil {
		return reply, err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "actor account is frozen"
		return reply, nil
	}

	targetAccount, err := getAccountEntry(tx, entry.TargetAccountID)
	if err != nil {
		return reply, err
	}
	if targetAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "target account is frozen"
		return reply, nil
	}

	if actorAccount.balanceCents < entry.AmountCents {
		reply.OK = false
		reply.Message = "insufficient funds"
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", actorAccount.balanceCents-entry.AmountCents, entry.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", targetAccount.balanceCents+entry.AmountCents, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Transferred %d cents to account %d", entry.AmountCents, entry.TargetAccountID)
	return reply, nil
}
