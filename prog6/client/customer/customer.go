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
		fmt.Println("\nCustomer Menu")
		fmt.Println("1) Check balance")
		fmt.Println("2) Deposit")
		fmt.Println("3) Withdraw")
		fmt.Println("4) Transfer")
		fmt.Println("q) Quit")
		fmt.Print("Choose an option: ")

		if !scanner.Scan() {
			fmt.Println("input closed")
			return
		}
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			acct := readInt64(scanner, "Account ID: ")
			req := common.CustomerRequest{RequestID: requestID("customer"), AccountID: acct}
			var resp common.CustomerReply
			callAndPrint(client, "Bank.CheckBalance", req, &resp)
			if resp.OK {
				fmt.Printf("Balance: %d cents\n", resp.Account.BalanceCents)
			}
		case "2":
			acct := readInt64(scanner, "Account ID: ")
			amt := readInt64(scanner, "Amount (cents): ")
			req := common.CustomerRequest{RequestID: requestID("customer"), AccountID: acct, AmountCents: amt}
			var resp common.CustomerReply
			callAndPrint(client, "Bank.Deposit", req, &resp)
			if resp.OK {
				fmt.Printf("New balance: %d cents\n", resp.NewBalanceCents)
			}
		case "3":
			acct := readInt64(scanner, "Account ID: ")
			amt := readInt64(scanner, "Amount (cents): ")
			req := common.CustomerRequest{RequestID: requestID("customer"), AccountID: acct, AmountCents: amt}
			var resp common.CustomerReply
			callAndPrint(client, "Bank.Withdraw", req, &resp)
			if resp.OK {
				fmt.Printf("New balance: %d cents\n", resp.NewBalanceCents)
			}
		case "4":
			from := readInt64(scanner, "From account ID: ")
			to := readInt64(scanner, "To account ID: ")
			amt := readInt64(scanner, "Amount (cents): ")
			req := common.CustomerRequest{RequestID: requestID("customer"), FromAccountID: from, ToAccountID: to, AmountCents: amt}
			var resp common.CustomerReply
			callAndPrint(client, "Bank.Transfer", req, &resp)
			if resp.OK {
				fmt.Printf("From balance: %d cents | To balance: %d cents\n", resp.FromNewBalanceCents, resp.ToNewBalanceCents)
			}
		case "q", "Q":
			fmt.Println("Goodbye.")
			return
		default:
			fmt.Println("Invalid option.")
		}
	}
}

func callAndPrint(client *rpc.Client, method string, req any, resp *common.CustomerReply) {
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
