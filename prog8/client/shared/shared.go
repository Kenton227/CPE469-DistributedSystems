package shared

import (
	"bufio"
	"fmt"
	"io"
	"math/rand/v2"
	"net/rpc"
	"os"
	"prog8/common"
	"strconv"
	"strings"
)

const RPCAddr = "localhost:9001"

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

func GetUsername(reader *bufio.Reader, prompt string) string {
	fmt.Print(prompt)
	username, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		fmt.Println(err)
		os.Exit(1)
	}
	return strings.TrimSpace(username)
}

func pickRandomServer() string {
	randomIdx := rand.IntN(len(common.SERVERS))
	return common.SERVERS[randomIdx]
}

// NOTE: caller must close client themselves!
func ConnectToBank() *rpc.Client {
	server := pickRandomServer()
	ipPort := fmt.Sprintf("%s:%s", server, common.BankPort)
	client, err := rpc.Dial("tcp", ipPort)
	if err != nil {
		fmt.Printf("failed to connect to server at %s: %v\n", ipPort, err)
		os.Exit(1)
	}
	fmt.Println("Connected to banking server.")
	return client
}

func RpcOperation(client *rpc.Client, req common.OperationRequest, reply *common.OperationReply) {
	if err := client.Call("Bank.DoOperation", req, reply); err != nil {
		fmt.Printf("RPC error (Bank.DoOperation): %v\n", err)
		return
	}
	if !reply.OK {
		fmt.Printf("%s failed: %s\n", req.Op, reply.Message)
		return
	}
	fmt.Println(reply.Message)
}
