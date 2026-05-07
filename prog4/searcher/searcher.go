package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"prog4/common"
	"strconv"
	"strings"
	"sync"
	"time"
)

const coordinatorAddr = "coordinator:1234"
const keywordFile = "/app/search_keywords.txt"

type threadStats struct {
	requests int
	success  int
	latencyNs int64
}

func main() {
	P := validArgs(os.Args)
	keywords := readKeywordsFromFile(keywordFile)
	outputMode := getSearchOutputMode()

	partitions := partitionKeywords(keywords, P)

	results := make(chan threadStats, P)
	var wg sync.WaitGroup
	for i := 0; i < P; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			results <- sendQueryPartition(threadID, partitions[threadID], outputMode)
		}(i)
	}

	wg.Wait()
	close(results)

	totalRequests := 0
	totalSuccess := 0
	var totalLatencyNs int64
	for st := range results {
		totalRequests += st.requests
		totalSuccess += st.success
		totalLatencyNs += st.latencyNs
	}

	if totalSuccess == 0 {
		fmt.Printf("AVG_SEARCH_LATENCY_MS N/A (requests=%d success=0)\n", totalRequests)
		return
	}

	avgMs := float64(totalLatencyNs) / float64(totalSuccess) / float64(time.Millisecond)
	fmt.Printf("Average search time in milliseconds: %.3f (requests=%d success=%d)\n", avgMs, totalRequests, totalSuccess)
}

// parses and validates the required P (thread count) command-line argument.
func validArgs(args []string) int {
	if len(args) < 2 {
		panic("usage: searcher <P>")
	}

	P, err := strconv.Atoi(args[1])
	if err != nil || P <= 0 {
		panic("error parsing P value")
	}

	return P
}

// reads SEARCH_OUTPUT_MODE and returns either "count" (default) or "list".
func getSearchOutputMode() string {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("SEARCH_OUTPUT_MODE")))
	if raw == "list" {
		return "list"
	}
	return "count"
}

// loads non-empty keywords from a file, normalizing them to lowercase.
func readKeywordsFromFile(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		panic(fmt.Sprintf("failed to open keyword file %s: %v", path, err))
	}
	defer f.Close()

	keywords := make([]string, 0)
	s := bufio.NewScanner(f)
	for s.Scan() {
		k := strings.ToLower(strings.TrimSpace(s.Text()))
		if k == "" {
			continue
		}
		keywords = append(keywords, k)
	}
	if err := s.Err(); err != nil {
		panic(fmt.Sprintf("failed to read keyword file %s: %v", path, err))
	}
	if len(keywords) == 0 {
		panic("search keyword file is empty")
	}
	return keywords
}

// splits the keyword list into chunks based on thread number
func partitionKeywords(keywords []string, threads int) [][]string {
	parts := make([][]string, threads)
	if threads <= 0 {
		return parts
	}

	base := len(keywords) / threads
	extra := len(keywords) % threads
	start := 0

	for i := 0; i < threads; i++ {
		size := base
		if i < extra {
			size++
		}
		end := start + size
		if end > len(keywords) {
			end = len(keywords)
		}
		parts[i] = keywords[start:end]
		start = end
	}

	return parts
}

// processes one thread's assigned keywords in order and prints each search result.
func sendQueryPartition(threadID int, keywords []string, outputMode string) threadStats {
	stats := threadStats{}
	for _, keyword := range keywords {
		stats.requests++
		start := time.Now()

		workerAddr, err := queryCoordinator(keyword)
		if err != nil {
			fmt.Printf("thread %d coordinator query failed for %q: %v\n", threadID, keyword, err)
			continue
		}

		urls, err := queryWorker(workerAddr, keyword)
		if err != nil {
			fmt.Printf("thread %d worker query failed for %q on %s: %v\n", threadID, keyword, workerAddr, err)
			continue
		}

		stats.success++
		stats.latencyNs += time.Since(start).Nanoseconds()
		if outputMode == "list" {
			fmt.Printf("thread %d keyword=%q worker=%s urls=%v\n", threadID, keyword, workerAddr, urls)
		} else {
			fmt.Printf("thread %d keyword=%q worker=%s urls=%d\n", threadID, keyword, workerAddr, len(urls))
		}
	}
	return stats
}

// asks the coordinator which worker should handle a single keyword search.
func queryCoordinator(keyword string) (string, error) {
	client, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		return "", err
	}
	defer client.Close()

	args := &common.SearchQueryArgs{Keyword: keyword}
	reply := &common.SearchQueryReply{}
	if err := client.Call("Coordinator.HandleSearchQuery", args, reply); err != nil {
		return "", err
	}
	if reply.HolderAddr == "" {
		return "", fmt.Errorf("no worker found for keyword %q", keyword)
	}

	return reply.HolderAddr, nil
}

// sends a keyword to the assigned worker and returns the URLs it reports.
func queryWorker(workerAddr string, keyword string) ([]string, error) {
	client, err := rpc.Dial("tcp", workerAddr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	args := &common.WorkerSearchArgs{Keyword: keyword}
	reply := &common.WorkerSearchReply{}
	if err := client.Call("Worker.HandleSearch", args, reply); err != nil {
		return nil, err
	}
	return reply.URLs, nil
}
