package main

import (
	"bufio"
	"strings"
	"fmt"
	"net/rpc"
	"os"
	"io"
	"prog6/common"
	"prog6/client/shared"
)

var username string
var accountID int64

func main() {
	client := shared.ConnectToBank()
	defer client.Close()

	// get user info
	reader := bufio.NewReader(os.Stdin)
	username = shared.GetUsername(reader, "Please enter your username: ")
	accountID, err := shared.GetAccountID(client, username)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("\nWelcome %s!\n", username)

	for {
		fmt.Println("\nCustomer Options")
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
			req := common.CustomerRequest{ActorAccountID: accountID}
			var reply common.CustomerReply
			rpcOperation(client, "Bank.CheckBalance", req, &reply)

		case "2":
			amt := shared.ReadInt64(reader, "Amount (cents): ")
			req := common.CustomerRequest{ActorAccountID: accountID, AmountCents: amt}
			var reply common.CustomerReply
			rpcOperation(client, "Bank.Deposit", req, &reply)

		case "3":
			amt := shared.ReadInt64(reader, "Amount (cents): ")
			req := common.CustomerRequest{ActorAccountID: accountID, AmountCents: amt}
			var reply common.CustomerReply
			rpcOperation(client, "Bank.Withdraw", req, &reply)

		case "4":
			targetUsername := shared.GetUsername(reader, "Enter target username: ")
			targetAccountID, err := shared.GetAccountID(client, targetUsername)
			if err != nil {
				fmt.Println(err)
				continue
			}
			amt := shared.ReadInt64(reader, "Transfer amount (cents): ")
			req := common.CustomerRequest{ActorAccountID: accountID, TargetAccountID: targetAccountID, AmountCents: amt}
			var reply common.CustomerReply
			rpcOperation(client, "Bank.Transfer", req, &reply)
		case "q", "Q":
			fmt.Println("Quitting...")
			return
		default:
			fmt.Println("Invalid option...")
		}
	}
}

func rpcOperation(client *rpc.Client, method string, req any, reply *common.CustomerReply) {
	if err := client.Call(method, req, reply); err != nil {
		fmt.Printf("RPC error (%s): %v\n", method, err)
		return
	}
	if !reply.OK {
		fmt.Printf("%s failed: %s\n", method, reply.Message)
		return
	}
	fmt.Println(reply.Message)
}
