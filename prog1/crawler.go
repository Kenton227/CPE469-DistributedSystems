package main

import (
	"fmt"
	"os"
	"time"
	"net/http"
	"golang.org/x/net/html"
)

const maxPages = 1
const logFile = "visitedUrls.txt"

/*
	Questions:
	do we record timestamp for each url
	should we add to url log each time we process url or at the end
	same question but for storing map in JSON file
*/

func main() {
	// store arguments as list
	urls := os.Args[1:]

	// start timer
	startTime := time.Now()

	// initialize inverted index and visited url map
	invIndex := make(map[string][]string)
	visitedUrls := make(map[string]bool)

	// iterate through urls, processing each
	visited := 0
	for len(urls) > 0 && visited < maxPages {
		urls = processUrl(urls, invIndex, visitedUrls, &visited)
	}

	// TODO: store invIndex in one JSON file
	// TODO: print first 10 keywords in index
	// TODO: record time needed to fetch and index content

	// end timer
	elapsedTime := time.Since(startTime)
	fmt.Println("Elapsed time:", elapsedTime)
}

/* stores each word from url into inverted index, updates visited, and returns new urls list*/
func processUrl(urls []string, invIndex map[string][]string, visitedUrls map[string]bool, visited *int) []string {
	// pop first url
	url := urls[0]
	urls = urls[1:]

	// check if url already processed
	if visitedUrls[url] {
		return urls
	}

	// mark url as visited
	visitedUrls[url] = true
	logUrl(url, visited)


	// TODO: remove
	fmt.Println("Processing:", url)

	// get response from GET request
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	
	// get parse tree from html body
	htmlNode, err := html.Parse(resp.Body)
	if err != nil {
		panic(err)
	}

	// process text from body
	processText(htmlNode, invIndex)

	// process links

	return urls
}

// TODO: add comment
func processText(htmlNode *html.Node, invIndex map[string][]string) {
	// TODO: extract text from htmlNode
	// TODO: pre process text data (lowercase, remove punctuation, remove "stop words")
	// TODO: store in inverted index; each word : url

}

/* logs url to logFile and increments visited counter */
// NOTE: maybe delete old log file on new crawler run
func logUrl(url string, visited *int) {
	*visited++

	// open file
	fptr, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer fptr.Close()

	// write url to file
	_, err = fptr.WriteString(url + "\n")
	if err != nil {
		panic(err)
	}
}
