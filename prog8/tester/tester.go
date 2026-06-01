package main

import (
	"fmt"
	"net"
	"net/rpc"
	"prog8/common"
)

var serverHosts = []string{"bankserver1", "bankserver2", "bankserver3"}

type Tester struct{}

func main() {
	tester := &Tester{}
	if err := rpc.Register(tester); err != nil {
		fmt.Println(err)
		return
	}

	listenAddr := fmt.Sprintf(":%s", common.TesterPort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()

	fmt.Printf("Tester ready on %s\n", listenAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (tester *Tester) CompareLogs(_ common.EmptyRequest, reply *common.CompareLogsReply) error {
	allLogs := make([][]common.LogEntry, 0, len(serverHosts))

	for _, host := range serverHosts {
		ipPort := fmt.Sprintf("%s:%s", host, common.BankPort)
		client, err := rpc.Dial("tcp", ipPort)
		if err != nil {
			reply.OK = false
			reply.Message = fmt.Sprintf("failed to connect to %s: %v", host, err)
			return nil
		}

		var entries []common.LogEntry
		callErr := client.Call("Bank.GetOperationsLog", common.EmptyRequest{}, &entries)
		client.Close()
		if callErr != nil {
			reply.OK = false
			reply.Message = fmt.Sprintf("failed to fetch logs from %s: %v", host, callErr)
			return nil
		}

		allLogs = append(allLogs, entries)
	}

	for i := 1; i < len(allLogs); i++ {
		if err := compareTwoLogs(allLogs[0], allLogs[i]); err != nil {
			reply.OK = false
			reply.Message = fmt.Sprintf("bankserver1 vs bankserver%d mismatch: %v", i+1, err)
			return nil
		}
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("all %d logs match (%d entries)", len(serverHosts), len(allLogs[0]))
	return nil
}

func compareTwoLogs(a []common.LogEntry, b []common.LogEntry) error {
	if len(a) != len(b) {
		return fmt.Errorf("entry count differs (%d vs %d)", len(a), len(b))
	}

	for i := range len(a) {
		if a[i] != b[i] {
			return fmt.Errorf("first mismatch at log index %d", a[i].LogIdx)
		}
	}

	return nil
}
