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

func main() {
	client := shared.ConnectToBank()
	defer client.Close()

	reader := bufio.NewReader(os.Stdin)

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
			username := shared.GetUsername(reader, "Enter target username: ")

			req := common.TellerRequest{Username: username}
			var reply common.TellerReply
			rpcOperation(client, "Bank.OpenAccount", req, &reply)

		case "2":
			username := shared.GetUsername(reader, "Enter target username: ")
			accountID, err := shared.GetAccountID(client, username)
			if err != nil {
				fmt.Println(err)
				continue
			}

			req := common.TellerRequest{TargetAccountID: accountID}
			var reply common.TellerReply
			rpcOperation(client, "Bank.CloseAccount", req, &reply)

		case "3":
			username := shared.GetUsername(reader, "Enter target username: ")
			accountID, err := shared.GetAccountID(client, username)
			if err != nil {
				fmt.Println(err)
				continue
			}

			req := common.TellerRequest{TargetAccountID: accountID}
			var reply common.TellerReply
			rpcOperation(client, "Bank.FreezeAccount", req, &reply)

		case "4":
			username := shared.GetUsername(reader, "Enter target username: ")
			accountID, err := shared.GetAccountID(client, username)
			if err != nil {
				fmt.Println(err)
				continue
			}

			req := common.TellerRequest{TargetAccountID: accountID}
			var reply common.TellerReply
			rpcOperation(client, "Bank.UnfreezeAccount", req, &reply)

		case "5":
			username := shared.GetUsername(reader, "Enter target username: ")
			accountID, err := shared.GetAccountID(client, username)
			if err != nil {
				fmt.Println(err)
				continue
			}

			bps := shared.ReadInt64(reader, "Percent in bps (ex: 250 = 2.50%, -125 = -1.25%): ")

			req := common.TellerRequest{TargetAccountID: accountID, PercentBPS: bps}
			var reply common.TellerReply
			rpcOperation(client, "Bank.ApplyRate", req, &reply)

		case "6":
			username := shared.GetUsername(reader, "Enter target username: ")
			accountID, err := shared.GetAccountID(client, username)
			if err != nil {
				fmt.Println(err)
				continue
			}

			fee := shared.ReadInt64(reader, "Service fee in cents (positive number): ")

			req := common.TellerRequest{TargetAccountID: accountID, AmountCents: fee}
			var reply common.TellerReply
			rpcOperation(client, "Bank.ChargeService", req, &reply)

		case "q", "Q":
			fmt.Println("Goodbye.")
			return

		default:
			fmt.Println("Invalid option.")
		}
	}
}

func rpcOperation(client *rpc.Client, method string, req any, reply *common.TellerReply) {
	if err := client.Call(method, req, reply); err != nil {
		fmt.Printf("RPC error (%s): %v\n", method, err)
		return
	}
	if !reply.OK {
		fmt.Printf("Operation failed: %s\n", reply.Message)
		return
	}
	fmt.Println(reply.Message)
}
