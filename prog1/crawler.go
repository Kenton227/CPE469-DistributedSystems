package main

import (
	"fmt"
	"os"
	"time"
	"net/http"
	"golang.org/x/net/html"
	"strings"
	"unicode"
	"bufio"
	"net/url"
)

const maxPages = 10000000
const logFile = "visitedUrls.txt"
const stopWordsFile = "stopWords.txt"


/*
	Questions:
	do we record timestamp for each url: record every factor of 10 timestamp
	should we add to url log each time we process url or at the end: do buffered writes every 1000 maybe
	same question but for storing map in JSON file: write JSON at end
	should we delete log file each time we run prog: yes
*/

// TODO: make the visitedUrls check only happen once, maybe change it to a queuedUrls check
func main() {
	// store arguments as list
	// NOTE: might want to change this to urlQueue
	urls := os.Args[1:]

	// start timer
	startTime := time.Now()

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
	//fmt.Println(invIndex)

	// TODO: store invIndex in one JSON file
	// TODO: print first 10 keywords in index
	// TODO: record time needed to fetch and index content

	// end timer
	elapsedTime := time.Since(startTime)
	fmt.Println("Elapsed time:", elapsedTime)
}

/* returns map of stop words from stopWordsFile */
func getStopWords() map[string]bool {
	// open file with stop words
	fptr, err := os.Open(stopWordsFile)
	// TODO: decide if we should quit or just return nothing
	if err != nil {
		panic(err)
	}
	defer fptr.Close()

	// create map of stop words
	stopWords := make(map[string]bool)
	scanner := bufio.NewScanner(fptr)
	for scanner.Scan() {
		stopWords[scanner.Text()] = true
	}

	return stopWords
}

/* stores each word from url into inverted index, updates visited, and returns new urls list*/
func processUrl(urls []string, invIndex map[string][]string, visitedUrls map[string]bool, visited *int, stopWords map[string]bool) []string {
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

	// process links
	processLinks(htmlNode, url, &urls, visitedUrls)

	return urls
}

// takes text from htmlNode and, after cleaning, adds the words to invIndex
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

/* recursively finds links from htmlNode and adds them to urls */
func processLinks(htmlNode *html.Node, url string, urls *[]string, visitedUrls map[string]bool) {
	// find each link and add it to urls to process
	if htmlNode.Type == html.ElementNode && htmlNode.Data == "a" {
		for _, attr := range htmlNode.Attr {
			if attr.Key == "href" {
				// get absolute url
				link := strings.TrimSpace(attr.Val)
				link = resolveLink(url, link)

				// add link to urls
				if link != "" && !visitedUrls[link]{
					*urls = append(*urls, link)
				}
			}
		}
	}

	// recursively process links
	for childNode := htmlNode.FirstChild; childNode != nil; childNode = childNode.NextSibling {
		processLinks(childNode, url, urls, visitedUrls)
	}
}

/* takes in a baseUrl and a link and returns the absolute path of the link */
func resolveLink(baseUrl string, link string) string {
	parsedBase, err := url.Parse(baseUrl)
	if err != nil {
		return ""
	}
	parsedLink, err := url.Parse(link)
	if err != nil {
		return ""
	}

	return parsedBase.ResolveReference(parsedLink).String()
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
