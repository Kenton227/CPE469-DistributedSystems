package main

import (
	"fmt"
	"net/rpc"
	"os"
	"prog8/common"
)

func main() {
	reply := &common.CompareLogsReply{}

	if err := CompareLogs(reply); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if reply.OK {
		fmt.Printf("OK: %s\n", reply.Message)
		os.Exit(0)
	}

	fmt.Printf("MISMATCH: %s\n", reply.Message)
	os.Exit(1)
}

func CompareLogs(reply *common.CompareLogsReply) error {
	allLogs := make([][]common.LogEntry, 0, len(common.SERVERS))

	for _, host := range common.SERVERS {
		ipPort := fmt.Sprintf("%s:%s", host, common.BankPort)

		client, err := rpc.Dial("tcp", ipPort)
		if err != nil {
			reply.OK = false
			reply.Message = fmt.Sprintf("failed to connect to %s: %v", host, err)
			continue
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
			reply.Message = fmt.Sprintf("%s vs %s mismatch: %v",
				common.SERVERS[0],
				common.SERVERS[i],
				err,
			)
			return nil
		}
	}

	reply.OK = true
	reply.Message = fmt.Sprintf("all %d logs match (%d entries)",
		len(common.SERVERS),
		len(allLogs[0]),
	)

	return nil
}

func compareTwoLogs(a []common.LogEntry, b []common.LogEntry) error {
	if len(a) != len(b) {
		return fmt.Errorf("entry count differs (%d vs %d)", len(a), len(b))
	}

	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf("first mismatch at slice index %d / log index %d",
				i,
				a[i].LogIdx,
			)
		}
	}

	return nil
}
