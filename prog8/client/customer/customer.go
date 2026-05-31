package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"prog7/client/shared"
	"prog7/common"
	"strings"
)

func main() {
	client := shared.ConnectToBank()
	defer client.Close()

	// get user info
	reader := bufio.NewReader(os.Stdin)
	actorUsername := sql.NullString{String: shared.GetUsername(reader, "Please enter your username: "), Valid: true}
	if actorUsername.String == "" {
		fmt.Println("username must contain at least 1 character")
		os.Exit(1)
	}
	fmt.Printf("\nWelcome %s!\n", actorUsername.String)

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

		switch choice {
		case "1":
			request := common.OperationRequest{Op: common.OpCheckBal, ActorUsername: actorUsername}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "2":
			amt := sql.NullInt64{Int64: shared.ReadInt64(reader, "Amount (cents): "), Valid: true}
			request := common.OperationRequest{Op: common.OpDeposit, ActorUsername: actorUsername, AmountCents: amt}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "3":
			amt := sql.NullInt64{Int64: shared.ReadInt64(reader, "Amount (cents): "), Valid: true}
			request := common.OperationRequest{Op: common.OpWithdraw, ActorUsername: actorUsername, AmountCents: amt}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "4":
			targetUsername := sql.NullString{String: shared.GetUsername(reader, "Enter target username: "), Valid: true}
			if actorUsername.String == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}
			amt := sql.NullInt64{Int64: shared.ReadInt64(reader, "Transfer amount (cents): "), Valid: true}
			request := common.OperationRequest{Op: common.OpTransfer, ActorUsername: actorUsername, TargetUsername: targetUsername, AmountCents: amt}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "q", "Q":
			fmt.Println("Quitting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid option...")
		}
	}
}
