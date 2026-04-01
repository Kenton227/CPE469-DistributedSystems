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

const maxPages = 1
const logFile = "visitedUrls.txt"
const stopWordsFile = "stopWords.txt"

/*
	Questions:
	do we record timestamp for each url
	should we add to url log each time we process url or at the end
	same question but for storing map in JSON file
	should we delete log file each time we run prog
*/

func main() {

	startTime := time.Now() // start timer

	urls := os.Args[1:]

	// initialize inverted index and visited url map
	invIndex := make(map[string][]string)
	visitedUrls := make(map[string]bool)
	stopWords := getStopWords()

	// iterate through urls, processing each
	visited := 0
	for len(urls) > 0 && visited < maxPages {
		urls = processUrl(urls, invIndex, visitedUrls, &visited, stopWords)
	}

	// TODO: remove
	fmt.Println(invIndex)

	// TODO: store invIndex in one JSON file
	// TODO: print first 10 keywords in index
	// TODO: record time needed to fetch and index content

	elapsedTime := time.Since(startTime) // end timer
	fmt.Println("Elapsed time:", elapsedTime)
}

/* Returns map of stop words from stopWordsFile. */
func getStopWords() map[string]bool {

	fptr, err := os.Open(stopWordsFile) // open file with stop words
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

/* stores each word from url into inverted index, updates visited, and returns new urls list*/
func processUrl(
	urls []string,
	invIndex map[string][]string,
	visitedUrls map[string]bool,
	visited *int,
	stopWords map[string]bool,
) []string {
	// pop first url
	url := urls[0]
	urls = urls[1:]

	// check if url already processed
	if visitedUrls[url] {
		return urls
	}

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
	processText(htmlNode, url, invIndex, stopWords)

	// TODO: process links

	return urls
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
