package main

import (
	"database/sql"
	"fmt"
	"prog7/common"
	"errors"

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

func (bank *Bank) getAccountID(username string, reply *common.OperationReply) (sql.NullInt64, error) {
	var accountID sql.NullInt64
	err := bank.db.QueryRow("SELECT account_id FROM accounts WHERE username = ?", username).Scan(&accountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			reply.OK = false
			reply.Message = fmt.Sprintf("Could not find username `%s`\n", username)
		}
		return accountID, err
	}
	accountID.Valid = true
	return accountID, nil
}

func getAccountEntry(tx *sql.Tx, accountID sql.NullInt64) (Account, error) {
	var acc Account

	if !accountID.Valid {
		return acc, errors.New("cannot get account from NULL id")
	}
	acc.accountId = accountID.Int64
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
		"UPDATE operations_log SET target_account_id = ? WHERE log_index = ?",
		newAccountID,
		entry.LogIdx,
	)
	if err != nil {
		return reply, err
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Opened account %d for %s", newAccountID, entry.TargetUsername.String)
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
	reply.Message = fmt.Sprintf("Closed account %d", entry.TargetAccountID.Int64)
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
	reply.Message = fmt.Sprintf("Froze account %d", entry.TargetAccountID.Int64)
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
	reply.Message = fmt.Sprintf("Unfroze account %d", entry.TargetAccountID.Int64)
	return reply, nil
}

func applyRate(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	targetAccount, err := getAccountEntry(tx, entry.TargetAccountID)
	if err != nil {
		return reply, err
	}

	newBalance := targetAccount.balanceCents + (targetAccount.balanceCents*entry.PercentBPS.Int64)/10000
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
	reply.Message = fmt.Sprintf("Applied %d bps; new balance: %d cents", entry.PercentBPS.Int64, newBalance)
	return reply, nil
}

func chargeService(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	targetAccount, err := getAccountEntry(tx, entry.TargetAccountID)
	if err != nil {
		return reply, err
	}
	if entry.AmountCents.Int64 > 0 {
		reply.OK = false
		reply.Message = "service fee can not be positive"
		return reply, nil
	}

	newBalance := targetAccount.balanceCents + entry.AmountCents.Int64
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
	reply.Message = fmt.Sprintf("Charged %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
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
	if entry.AmountCents.Int64 <= 0 {
		reply.OK = false
		reply.Message = "deposit amount must be positive"
		return reply, nil
	}

	newBalance := actorAccount.balanceCents + entry.AmountCents.Int64
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
	reply.Message = fmt.Sprintf("Deposited %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
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
	if entry.AmountCents.Int64 <= 0 {
		reply.OK = false
		reply.Message = "withdraw amount must be positive"
		return reply, nil
	}

	newBalance := actorAccount.balanceCents - entry.AmountCents.Int64
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
	reply.Message = fmt.Sprintf("Withdrew %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
	return reply, nil
}

func transfer(tx *sql.Tx, entry common.LogEntry) (common.OperationReply, error) {
	var reply common.OperationReply

	if entry.AmountCents.Int64 <= 0 {
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

	if actorAccount.balanceCents < entry.AmountCents.Int64 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", actorAccount.balanceCents-entry.AmountCents.Int64, entry.ActorAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE account_id = ?", targetAccount.balanceCents+entry.AmountCents.Int64, entry.TargetAccountID)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return reply, nil
	}

	if err := tx.Commit(); err != nil {
		return reply, err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Transferred %d cents to account %d", entry.AmountCents.Int64, entry.TargetAccountID.Int64)
	return reply, nil
}
