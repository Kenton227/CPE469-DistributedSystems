package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"prog8/client/shared"
	"prog8/common"
	"strings"
)

func sendRequest(conn *shared.BankConnection, requestDB *sql.DB, req common.OperationRequest) {
	var reply common.OperationReply
	fulfilled := shared.RpcOperation(conn, req, &reply)

	if fulfilled {
		if err := shared.AdvanceRequestID(requestDB, req.ActorUsername); err != nil {
			fmt.Println(err)
		}
	}
}

func autoOpenAccounts(conn *shared.BankConnection, requestDB *sql.DB, actorUsername string) {
	fmt.Println("Auto-opening test accounts...")

	for _, username := range shared.TEST_USERS {
		requestID, err := shared.GetNextRequestID(requestDB, actorUsername)
		if err != nil {
			fmt.Println(err)
			continue
		}

		req := common.OperationRequest{
			RequestID:     requestID,
			Op:            common.OpOpen,
			ActorUsername: actorUsername,
			TargetUsername: sql.NullString{
				String: username,
				Valid:  true,
			},
		}

		fmt.Printf("Opening account %s with requestID=%d\n", username, requestID)
		sendRequest(conn, requestDB, req)
	}
}

func main() {
	autoOpen := flag.Bool("auto-open", false, "open default test accounts and exit")
	autoUser := flag.String("user", "teller", "teller username for auto mode")
	flag.Parse()

	requestDB, err := shared.InitClientRequestDB("db/client_requests.db")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer requestDB.Close()

	conn := shared.ConnectToBank()
	defer conn.Client.Close()

	if *autoOpen {
		autoOpenAccounts(conn, requestDB, *autoUser)
		return
	}

	reader := bufio.NewReader(os.Stdin)

	actorUsername := shared.GetUsername(reader, "Please enter your username: ")
	if actorUsername == "" {
		fmt.Println("username must contain at least 1 character")
		os.Exit(1)
	}
	fmt.Printf("\nWelcome %s!\n", actorUsername)

	for {
		fmt.Println("\nTeller Menu")
		fmt.Println("1) Open account")
		fmt.Println("2) Close account")
		fmt.Println("3) Freeze account")
		fmt.Println("4) Unfreeze account")
		fmt.Println("5) Apply bonus/interest")
		fmt.Println("6) Charge service fee")
		fmt.Println("q) Quit")
		fmt.Print("Choose an option: ")

		choice, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err)
			os.Exit(1)
		}
		choice = strings.TrimSpace(choice)

		if choice == "q" || choice == "Q" {
			fmt.Println("Quitting...")
			os.Exit(0)
		}

		if choice != "1" && choice != "2" && choice != "3" &&
			choice != "4" && choice != "5" && choice != "6" {
			fmt.Println("Invalid option.")
			continue
		}

		requestID, err := shared.GetNextRequestID(requestDB, actorUsername)
		if err != nil {
			fmt.Println(err)
			continue
		}

		var request common.OperationRequest
		validRequest := true

		switch choice {
		case "1":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpOpen,
				ActorUsername:  actorUsername,
				TargetUsername: username,
			}

		case "2":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpClose,
				ActorUsername:  actorUsername,
				TargetUsername: username,
			}

		case "3":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpFreeze,
				ActorUsername:  actorUsername,
				TargetUsername: username,
			}

		case "4":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpUnfreeze,
				ActorUsername:  actorUsername,
				TargetUsername: username,
			}

		case "5":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			bps := sql.NullInt64{
				Int64: shared.ReadInt64(reader, "Percent in bps (ex: 250 = 2.50%, -125 = -1.25%): "),
				Valid: true,
			}

			op := common.OpBonus
			if bps.Int64 < 0 {
				op = common.OpInterest
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             op,
				ActorUsername:  actorUsername,
				TargetUsername: username,
				PercentBPS:     bps,
			}

		case "6":
			username := sql.NullString{
				String: shared.GetUsername(reader, "Enter target username: "),
				Valid:  true,
			}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			fee := sql.NullInt64{
				Int64: shared.ReadInt64(reader, "Service fee in cents (negative number): "),
				Valid: true,
			}

			request = common.OperationRequest{
				RequestID:      requestID,
				Op:             common.OpChargeService,
				ActorUsername:  actorUsername,
				TargetUsername: username,
				AmountCents:    fee,
			}

		default:
			validRequest = false
		}

		if !validRequest {
			continue
		}

		sendRequest(conn, requestDB, request)
	}
}
