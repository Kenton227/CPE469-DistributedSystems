package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"prog8/client/shared"
	"prog8/common"
	"strings"
)

func main() {
	conn := shared.ConnectToBank()
	defer conn.Client.Close()

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

		switch choice {
		case "1":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpOpen, ActorUsername: actorUsername, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "2":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpClose, ActorUsername: actorUsername, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "3":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpFreeze, ActorUsername: actorUsername, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "4":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpUnfreeze, ActorUsername: actorUsername, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "5":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			bps := sql.NullInt64{Int64: shared.ReadInt64(reader, "Percent in bps (ex: 250 = 2.50%, -125 = -1.25%): "), Valid: true}

			request := common.OperationRequest{Op: common.OpBonus, ActorUsername: actorUsername, TargetUsername: username, PercentBPS: bps}
			if bps.Int64 < 0 {
				request.Op = common.OpInterest
			}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "6":
			username := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if username.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			fee := sql.NullInt64{Int64: shared.ReadInt64(reader, "Service fee in cents (negative number): "), Valid: true}

			request := common.OperationRequest{Op: common.OpChargeService, ActorUsername: actorUsername, TargetUsername: username, AmountCents: fee}
			var reply common.OperationReply
			shared.RpcOperation(conn, request, &reply)

		case "q", "Q":
			fmt.Println("Quitting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid option.")
		}
	}
}
