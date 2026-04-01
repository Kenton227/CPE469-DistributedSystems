package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"bufio"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

const MAX_PAGES = 10 ^ 7
const FIRST_MILESTONE = 100
const MILESTONE_GROWTH_FCTR = 10
const LOGFILE = "visitedUrls.txt"
const STOPWORDSFILE = "stopWords.txt"

/*
	Questions:
	do we record timestamp for each url
	should we add to url log each time we process url or at the end
	same question but for storing map in JSON file
	should we delete log file each time we run prog
*/

func printTimeSinceStart(start time.Time) {
	fmt.Println("Elapsed time:", time.Since(start))
}

func main() {

	startTime := time.Now() // start timer
	defer printTimeSinceStart(startTime)

	invIndex := make(map[string][]string) // init inverted index and stopwords
	stopWords := getStopWords()

	// init LOGFILE
	fptr, err := os.OpenFile(LOGFILE, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer fptr.Close()
	log := bufio.NewWriter(fptr)
	defer log.Flush()

	// iterate through urls, processing each
	urls := os.Args[1:]
	processUrls(urls, invIndex, stopWords, log, startTime)

	// TODO: remove
	// fmt.Println(invIndex)

	// TODO: store invIndex in one JSON file
	// TODO: print first 10 keywords in index
	// TODO: record time needed to fetch and index content

}

/* Returns map of stop words from STOPWORDSFILE. */
func getStopWords() map[string]bool {

	fptr, err := os.Open(STOPWORDSFILE) // open file with stop words
	if err != nil {                     // TODO: decide if we should quit or just return nothing
		panic(err)
	}
	defer fptr.Close()

	stopWords := make(map[string]bool)
	scanner := bufio.NewScanner(fptr)
	for scanner.Scan() {
		stopWords[scanner.Text()] = true
	}

	return stopWords
}

/* stores each word from url into inverted index, updates visited */
func processUrls(
	urls []string,
	invIndex map[string][]string,
	stopWords map[string]bool,
	log *bufio.Writer,
	startTime time.Time,
) {

	visitedUrls := make(map[string]bool)
	visited := 0
	nextMilestone := FIRST_MILESTONE
	for len(urls) > 0 && visited < MAX_PAGES {
		url := urls[0] // pop first url
		urls = urls[1:]

		// check if url already processed
		if visitedUrls[url] {
			continue
		}

		visitedUrls[url] = true
		visited++
		logUrl(url, log)

		fmt.Println("Processing:", url) // TODO: remove

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

		processText(htmlNode, url, invIndex, stopWords)

		// TODO: process links

		if visited == nextMilestone {
			printTimeSinceStart(startTime)
			nextMilestone *= MILESTONE_GROWTH_FCTR
		}
	}
	fmt.Println("Visited:", visited)
}

// TODO: add comment
func processText(htmlNode *html.Node, url string, invIndex map[string][]string, stopWords map[string]bool) {
	// extract text if node is TextNode
	if htmlNode.Type == html.TextNode {
		// clean data string
		cleanData := removePunct(strings.ToLower(htmlNode.Data))

		// convert data string to word list
		words := strings.Fields(cleanData)

		// store words in invIndex
		for _, word := range words {
			// dont include stop words
			if !stopWords[word] {
				addToInvIndex(word, url, invIndex)
			}
		}
	}

	// recursively process children
	for childNode := htmlNode.FirstChild; childNode != nil; childNode = childNode.NextSibling {
		processText(childNode, url, invIndex, stopWords)
	}
}

/* adds word : url to invIndex while avoiding duplicate urls */
func addToInvIndex(word string, url string, invIndex map[string][]string) {
	// check if duplicate url
	valueUrls := invIndex[word]
	for _, valueUrl := range valueUrls {
		if valueUrl == url {
			return
			fmt.Println("DUPLICATE URL")
		}
	}

	// add url to invIndex
	invIndex[word] = append(valueUrls, url)
}

/* returns inputted string with punctuation removed */
func removePunct(str string) string {
	runes := []rune{}

	// append character if not punctuation
	for _, run := range str {
		if !unicode.IsPunct(run) {
			runes = append(runes, run)
		}
	}

	return string(runes)
}

/* logs url to LOGFILE */
func logUrl(url string, log *bufio.Writer) {
	_, err := log.WriteString(url + "\n")
	if err != nil {
		panic(err)
	}
}
