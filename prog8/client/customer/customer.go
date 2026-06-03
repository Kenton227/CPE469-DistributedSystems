package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"prog8/client/shared"
	"prog8/common"
	"strings"
	"time"
)

func getNextRequest(requestDB *sql.DB, actorUsername string) (int64, error) {
	return shared.GetNextRequestID(requestDB, actorUsername)
}

func sendRequest(conn *shared.BankConnection, requestDB *sql.DB, req common.OperationRequest) {
	var reply common.OperationReply
	fulfilled := shared.RpcOperation(conn, req, &reply)

	if fulfilled {
		if err := shared.AdvanceRequestID(requestDB, req.ActorUsername); err != nil {
			fmt.Println(err)
		}
	}
}

func randomCustomerRequest(requestDB *sql.DB, actorUsername string) (common.OperationRequest, error) {
	requestID, err := getNextRequest(requestDB, actorUsername)
	if err != nil {
		return common.OperationRequest{}, err
	}

	opChoice := rand.IntN(4)

	req := common.OperationRequest{
		RequestID:     requestID,
		ActorUsername: actorUsername,
	}

	switch opChoice {
	case 0:
		req.Op = common.OpCheckBal

	case 1:
		req.Op = common.OpDeposit
		req.AmountCents = sql.NullInt64{
			Int64: int64(rand.IntN(10000) + 1),
			Valid: true,
		}

	case 2:
		req.Op = common.OpWithdraw
		req.AmountCents = sql.NullInt64{
			Int64: int64(rand.IntN(5000) + 1),
			Valid: true,
		}

	case 3:
		target := actorUsername
		for target == actorUsername {
			target = shared.TEST_USERS[rand.IntN(len(shared.TEST_USERS))]
		}

		req.Op = common.OpTransfer
		req.TargetUsername = sql.NullString{
			String: target,
			Valid:  true,
		}
		req.AmountCents = sql.NullInt64{
			Int64: int64(rand.IntN(5000) + 1),
			Valid: true,
		}
	}

	return req, nil
}

func runAutoMode(conn *shared.BankConnection, requestDB *sql.DB, actorUsername string, numRequests int, delayMS int) {
	fmt.Printf("Running %d random customer requests as %s...\n", numRequests, actorUsername)

	for i := 0; i < numRequests; i++ {
		req, err := randomCustomerRequest(requestDB, actorUsername)
		if err != nil {
			fmt.Println(err)
			continue
		}

		fmt.Printf("AUTO request %d: %s requestID=%d\n", i+1, req.Op, req.RequestID)

		sendRequest(conn, requestDB, req)

		if delayMS > 0 {
			time.Sleep(time.Duration(delayMS) * time.Millisecond)
		}
	}
}

func runInteractiveMode(conn *shared.BankConnection, requestDB *sql.DB) {
	reader := bufio.NewReader(os.Stdin)
	actorUsername := shared.GetUsername(reader, "Please enter your username: ")
	if actorUsername == "" {
		fmt.Println("username must contain at least 1 character")
		os.Exit(1)
	}
	fmt.Printf("\nWelcome %s!\n", actorUsername)

	for {
		fmt.Println("\nCustomer Menu")
		fmt.Println("1) Check balance")
		fmt.Println("2) Deposit")
		fmt.Println("3) Withdraw")
		fmt.Println("4) Transfer")
		fmt.Println("q) Quit")
		fmt.Print("Choose an option: ")

		choice, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err)
			os.Exit(1)
		}
		choice = strings.TrimSpace(choice)

		var request common.OperationRequest
		validRequest := true

		requestID, err := getNextRequest(requestDB, actorUsername)
		if err != nil {
			fmt.Println(err)
			continue
		}

		switch choice {
		case "1":
			request = common.OperationRequest{
				RequestID:     requestID,
				Op:            common.OpCheckBal,
				ActorUsername: actorUsername,
			}

		case "2":
			amt := sql.NullInt64{
				Int64: shared.ReadInt64(reader, "Amount (cents): "),
				Valid: true,
			}
			request = common.OperationRequest{
				RequestID:     requestID,
				Op:            common.OpDeposit,
				ActorUsername: actorUsername,
				AmountCents:   amt,
			}

		case "3":
			amt := sql.NullInt64{
				Int64: shared.ReadInt64(reader, "Amount (cents): "),
				Valid: true,
			}
			request = common.OperationRequest{
				RequestID:     requestID,
				Op:            common.OpWithdraw,
				ActorUsername: actorUsername,
				AmountCents:   amt,
			}

		case "4":
			targetUsername := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if targetUsername.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			amt := sql.NullInt64{
				Int64: shared.ReadInt64(reader, "Transfer amount (cents): "),
				Valid: true,
			}
			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpTransfer,
				ActorUsername:  actorUsername,
				TargetUsername: targetUsername,
				AmountCents:    amt,
			}

		case "q", "Q":
			fmt.Println("Quitting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid option...")
			validRequest = false
		}

		if !validRequest {
			continue
		}

		sendRequest(conn, requestDB, request)
	}
}

func main() {
	autoMode := flag.Bool("auto", false, "run randomized customer workload")
	autoUser := flag.String("user", "alice", "username for auto mode")
	numRequests := flag.Int("n", 100, "number of requests in auto mode")
	delayMS := flag.Int("delay-ms", 25, "delay between auto requests in milliseconds")
	flag.Parse()

	requestDB, err := shared.InitClientRequestDB("db/client_requests.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer requestDB.Close()

	conn := shared.ConnectToBank()
	defer conn.Client.Close()

	if *autoMode {
		runAutoMode(conn, requestDB, *autoUser, *numRequests, *delayMS)
		return
	}

	runInteractiveMode(conn, requestDB)
}
