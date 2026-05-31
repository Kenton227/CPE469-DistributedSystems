package shared

import (
	"fmt"
	"strings"
	"os"
	"bufio"
	"strconv"
	"errors"
	"io"
	"net/rpc"
	"prog6/common"
)

const RPCAddr = "localhost:9000"

func ReadInt64(reader *bufio.Reader, prompt string) int64 {
	for {
		fmt.Print(prompt)
		inputNum, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err)
			os.Exit(1)
		}
		inputNum = strings.TrimSpace(inputNum)
		num, err := strconv.ParseInt(inputNum, 10, 64)
		if err == nil {
			return num
		}
		fmt.Println("Please enter a valid integer.")
	}
}

func GetAccountID(client *rpc.Client, username string) (int64, error) {
	req := common.GetIDRequest{Username: username}
	var resp common.GetIDReply
	if err := client.Call("Bank.GetAccountID", req, &resp); err != nil {
		fmt.Printf("RPC error: %v\n", err)
		os.Exit(1)
	}
	if !resp.OK {
		return -1, errors.New(resp.ErrorMsg)
	}
	return resp.AccountID, nil
}

func GetUsername(reader *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	username, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.TrimSpace(username)
}

// NOTE: caller must close client themselves!
func ConnectToBank() *rpc.Client {
	ipPort := fmt.Sprintf("localhost:%s", common.BankPort)
	client, err := rpc.Dial("tcp", ipPort)
	if err != nil {
		fmt.Printf("failed to connect to server at %s: %v\n", ipPort, err)
		os.Exit(1)
	}
	fmt.Println("Connected to banking server.")
	return client
}
