package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
	"prog6/common"
)

const RPCAddr = "127.0.0.1:9000"

func main() {
	client, err := rpc.Dial("tcp", RPCAddr)
	if err != nil {
		fmt.Printf("failed to connect to server at %s: %v\n", RPCAddr, err)
		os.Exit(1)
	}
	defer client.Close()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Connected to banking server.")

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

		if !scanner.Scan() {
			fmt.Println("input closed")
			return
		}
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			fmt.Print("Username: ")
			if !scanner.Scan() {
				return
			}
			username := strings.TrimSpace(scanner.Text())
			req := common.TellerRequest{RequestID: requestID("teller"), Username: username}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.OpenAccount", req, &resp)
			if resp.OK {
				fmt.Printf("Opened account: id=%d username=%s\n", resp.Account.AccountID, resp.Account.Username)
			}
		case "2":
			acct := readInt64(scanner, "Account ID: ")
			req := common.TellerRequest{RequestID: requestID("teller"), AccountID: acct}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.CloseAccount", req, &resp)
		case "3":
			acct := readInt64(scanner, "Account ID: ")
			req := common.TellerRequest{RequestID: requestID("teller"), AccountID: acct}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.FreezeAccount", req, &resp)
		case "4":
			acct := readInt64(scanner, "Account ID: ")
			req := common.TellerRequest{RequestID: requestID("teller"), AccountID: acct}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.UnfreezeAccount", req, &resp)
		case "5":
			acct := readInt64(scanner, "Account ID: ")
			bps := readInt64(scanner, "Percent in bps (ex: 250 = 2.50%, -125 = -1.25%): ")
			op := "bonus"
			if bps < 0 {
				op = "interest"
			}
			req := common.TellerRequest{RequestID: requestID("teller"), AccountID: acct, PercentBPS: bps, Operation: op}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.ApplyRate", req, &resp)
			if resp.OK {
				fmt.Printf("New balance: %d cents\n", resp.NewBalanceCents)
			}
		case "6":
			acct := readInt64(scanner, "Account ID: ")
			amount := readInt64(scanner, "Service fee in cents (positive number): ")
			req := common.TellerRequest{RequestID: requestID("teller"), AccountID: acct, AmountCents: amount, Operation: "charge_service"}
			var resp common.TellerReply
			callAndPrintTeller(client, "Bank.ChargeService", req, &resp)
			if resp.OK {
				fmt.Printf("New balance: %d cents\n", resp.NewBalanceCents)
			}
		case "q", "Q":
			fmt.Println("Goodbye.")
			return
		default:
			fmt.Println("Invalid option.")
		}
	}
}

func callAndPrintTeller(client *rpc.Client, method string, req any, resp *common.TellerReply) {
	if err := client.Call(method, req, resp); err != nil {
		fmt.Printf("RPC error (%s): %v\n", method, err)
		return
	}
	if !resp.OK {
		fmt.Printf("Operation failed: %s (%s)\n", resp.Message, resp.ErrorCode)
		return
	}
	fmt.Println("Operation succeeded.")
}

func readInt64(scanner *bufio.Scanner, prompt string) int64 {
	for {
		fmt.Print(prompt)
		if !scanner.Scan() {
			fmt.Println("input closed")
			os.Exit(0)
		}
		v := strings.TrimSpace(scanner.Text())
		n, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return n
		}
		fmt.Println("Please enter a valid integer.")
	}
}

func requestID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
