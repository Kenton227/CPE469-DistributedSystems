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

type BankConnection struct {
	Client *rpc.Client
	Server string
}

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

func dialServer(server string) (*rpc.Client, error) {
	ipPort := fmt.Sprintf("%s:%s", server, common.BankPort)
	return rpc.Dial("tcp", ipPort)
}

// NOTE: caller must close conn.Client themselves!
func ConnectToBank() *BankConnection {
	server := pickRandomServer()

	client, err := dialServer(server)
	if err != nil {
		fmt.Printf("failed to connect to server %s: %v\n", server, err)
		os.Exit(1)
	}

	fmt.Printf("Connected to banking server %s.\n", server)

	return &BankConnection{
		Client: client,
		Server: server,
	}
}

func RpcOperation(conn *BankConnection, req common.OperationRequest, reply *common.OperationReply) {
	if err := conn.Client.Call("Bank.DoOperation", req, reply); err != nil {
		fmt.Printf("RPC error (Bank.DoOperation): %v\n", err)
		return
	}

	if !reply.OK && reply.LeaderAddr != "" && reply.LeaderAddr != conn.Server {
		fmt.Printf("Redirected to leader %s.\n", reply.LeaderAddr)

		conn.Client.Close()

		newClient, err := dialServer(reply.LeaderAddr)
		if err != nil {
			fmt.Printf("failed to connect to leader %s: %v\n", reply.LeaderAddr, err)
			return
		}

		conn.Client = newClient
		conn.Server = reply.LeaderAddr

		// Retry once on the correct leader.
		if err := conn.Client.Call("Bank.DoOperation", req, reply); err != nil {
			fmt.Printf("RPC error after redirect (Bank.DoOperation): %v\n", err)
			return
		}
	}

	if !reply.OK {
		fmt.Printf("%s failed: %s\n", req.Op, reply.Message)
		return
	}

	fmt.Println(reply.Message)
}
