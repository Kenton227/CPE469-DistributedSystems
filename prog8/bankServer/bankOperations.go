package main

import (
	"database/sql"
	"errors"
	"fmt"
	"prog8/common"

	_ "github.com/mattn/go-sqlite3"
)

type Status string

const (
	StatusActive Status = "active"
	StatusFrozen Status = "frozen"
)

type Account struct {
	accountId    int64
	username     string
	balanceCents int64
	status       Status
}

func (bank *Bank) getAccountID(username string, reply *common.OperationReply) (sql.NullInt64, error) {
	var accountID sql.NullInt64
	err := bank.db.QueryRow("SELECT account_id FROM accounts WHERE username = ?", username).Scan(&accountID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			reply.OK = false
			reply.Message = fmt.Sprintf("could not find username `%s`", username)
			return accountID, nil
		}
		return accountID, err
	}
	accountID.Valid = true
	reply.OK = true
	return accountID, nil
}

func getAccountEntry(tx *sql.Tx, targetUsername sql.NullString) (Account, error) {
	var acc Account

	if !targetUsername.Valid {
		return acc, errors.New("cannot get account from NULL username")
	}
	acc.username = targetUsername.String
	err := tx.QueryRow(`
			SELECT username, balance_cents, status
			FROM accounts
			WHERE username = ?`,
		targetUsername).Scan(&acc.username, &acc.balanceCents, &acc.status)
	if err != nil {
		return acc, err
	}
	return acc, nil
}

func openAccount(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	accountResult, err := tx.Exec(
		"INSERT INTO accounts(username, balance_cents, status) VALUES (?, 0, ?)",
		entry.TargetUsername,
		StatusActive,
	)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	newAccountID, err := accountResult.LastInsertId()
	if err != nil {
		return err
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Opened account %d for %s", newAccountID, entry.TargetUsername.String)
	return nil
}

func closeAccount(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	deleteResult, err := tx.Exec("DELETE FROM accounts WHERE username = ?", entry.TargetUsername)
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

	reply.OK = true
	reply.Message = fmt.Sprintf("Closed account %s", entry.TargetUsername.String)
	return nil
}

func freezeAccount(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE username = ?", StatusFrozen, entry.TargetUsername)
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

	reply.OK = true
	reply.Message = fmt.Sprintf("Froze account %s", entry.TargetUsername.String)
	return nil
}

func unfreezeAccount(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	res, err := tx.Exec("UPDATE accounts SET status = ? WHERE username = ?", StatusActive, entry.TargetUsername)
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

	reply.OK = true
	reply.Message = fmt.Sprintf("Unfroze account %s", entry.TargetUsername.String)
	return nil
}

func applyRate(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	targetAccount, err := getAccountEntry(tx, entry.TargetUsername)
	if err != nil {
		return err
	}

	newBalance := targetAccount.balanceCents + (targetAccount.balanceCents*entry.PercentBPS.Int64)/10000
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "operation would make balance negative"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", newBalance, entry.TargetUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Applied %d bps; new balance: %d cents", entry.PercentBPS.Int64, newBalance)
	return nil
}

func chargeService(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	targetAccount, err := getAccountEntry(tx, entry.TargetUsername)
	if err != nil {
		return err
	}
	if entry.AmountCents.Int64 > 0 {
		reply.OK = false
		reply.Message = "service fee can not be positive"
		return nil
	}

	newBalance := targetAccount.balanceCents + entry.AmountCents.Int64
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", newBalance, entry.TargetUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Charged %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
	return nil
}

func checkBalance(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	actorAccount, err := getAccountEntry(tx, sql.NullString{String: entry.ActorUsername, Valid: true})
	if err != nil {
		return err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Current balance: %d cents", actorAccount.balanceCents)
	return nil
}

func deposit(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	actorAccount, err := getAccountEntry(tx, sql.NullString{String: entry.ActorUsername, Valid: true})
	if err != nil {
		return err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}
	if entry.AmountCents.Int64 <= 0 {
		reply.OK = false
		reply.Message = "deposit amount must be positive"
		return nil
	}

	newBalance := actorAccount.balanceCents + entry.AmountCents.Int64
	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", newBalance, entry.ActorUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Deposited %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
	return nil
}

func withdraw(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	actorAccount, err := getAccountEntry(tx, sql.NullString{String: entry.ActorUsername, Valid: true})
	if err != nil {
		return err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "account is frozen"
		return nil
	}
	if entry.AmountCents.Int64 <= 0 {
		reply.OK = false
		reply.Message = "withdraw amount must be positive"
		return nil
	}

	newBalance := actorAccount.balanceCents - entry.AmountCents.Int64
	if newBalance < 0 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", newBalance, entry.ActorUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Withdrew %d cents; new balance: %d cents", entry.AmountCents.Int64, newBalance)
	return nil
}

func transfer(tx *sql.Tx, entry common.LogEntry, reply *common.OperationReply) error {

	if entry.AmountCents.Int64 <= 0 {
		reply.OK = false
		reply.Message = "transfer amount must be positive"
		return nil
	}
	if entry.TargetUsername.Valid && entry.ActorUsername == entry.TargetUsername.String {
		reply.OK = false
		reply.Message = "cannot transfer to same account"
		return nil
	}

	actorAccount, err := getAccountEntry(tx, sql.NullString{String: entry.ActorUsername, Valid: true})
	if err != nil {
		return err
	}
	if actorAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "actor account is frozen"
		return nil
	}

	targetAccount, err := getAccountEntry(tx, entry.TargetUsername)
	if err != nil {
		return err
	}
	if targetAccount.status == StatusFrozen {
		reply.OK = false
		reply.Message = "target account is frozen"
		return nil
	}

	if actorAccount.balanceCents < entry.AmountCents.Int64 {
		reply.OK = false
		reply.Message = "insufficient funds"
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", actorAccount.balanceCents-entry.AmountCents.Int64, entry.ActorUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	_, err = tx.Exec("UPDATE accounts SET balance_cents = ? WHERE username = ?", targetAccount.balanceCents+entry.AmountCents.Int64, entry.TargetUsername)
	if err != nil {
		reply.OK = false
		reply.Message = fmt.Sprintf("%v", err)
		return nil
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("Transferred %d cents to account %s", entry.AmountCents.Int64, entry.TargetUsername.String)
	return nil
}
