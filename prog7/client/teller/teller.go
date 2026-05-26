package main

import (
	"bufio"
	"strings"
	"fmt"
	"os"
	"io"
	"prog7/common"
	"prog7/client/shared"
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
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpOpen, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "2":
			username := shared.GetUsername(reader, "Enter target username: ")
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpClose, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "3":
			username := shared.GetUsername(reader, "Enter target username: ")
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpFreeze, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "4":
			username := shared.GetUsername(reader, "Enter target username: ")
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			request := common.OperationRequest{Op: common.OpUnfreeze, TargetUsername: username}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "5":
			username := shared.GetUsername(reader, "Enter target username: ")
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			bps := shared.ReadInt64(reader, "Percent in bps (ex: 250 = 2.50%, -125 = -1.25%): ")

			request := common.OperationRequest{Op: common.OpBonus, TargetUsername: username, PercentBPS: bps}
			if bps < 0 {
				request.Op = common.OpInterest
			}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "6":
			username := shared.GetUsername(reader, "Enter target username: ")
			if username == "" {
				fmt.Println("username must contain at least 1 character")
				continue
			}

			fee := shared.ReadInt64(reader, "Service fee in cents (negative number): ")

			request := common.OperationRequest{Op: common.OpChargeService, TargetUsername: username, AmountCents: fee}
			var reply common.OperationReply
			shared.RpcOperation(client, request, &reply)

		case "q", "Q":
			fmt.Println("Quitting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid option.")
		}
	}
}
