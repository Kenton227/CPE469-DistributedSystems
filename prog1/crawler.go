package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"bufio"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

const MAX_PAGES = 10000000
const FIRST_MILESTONE = 100
const MILESTONE_GROWTH_FCTR = 10
const LOGFILE = "visitedUrls.txt"
const STOPWORDSFILE = "stopWords.txt"

/*
	Questions:
	do we record timestamp for each url: record every factor of 10 timestamp
	should we add to url log each time we process url or at the end: do buffered writes every 1000 maybe
	same question but for storing map in JSON file: write JSON at end
	should we delete log file each time we run prog: yes
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
	// NOTE: may want to filter out duplicates from command line arguments
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

	// initialized queued urls map
	queuedUrls := make(map[string]bool)
	for _, url := range urls {
		queuedUrls[url] = true
	}

	visited := 0
	nextMilestone := FIRST_MILESTONE
	for len(urls) > 0 && visited < MAX_PAGES {
		url := urls[0] // pop first url
		urls = urls[1:]

		visited++
		logUrl(url, log)

		//fmt.Println("Processing:", url) // TODO: remove

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

		// processing data
		processText(htmlNode, url, invIndex, stopWords)
		processLinks(htmlNode, url, &urls, queuedUrls)

		if visited == nextMilestone {
			printTimeSinceStart(startTime)
			nextMilestone *= MILESTONE_GROWTH_FCTR
			// TODO: remove
			fmt.Println("Visited:", visited)
		}

		// TODO: remove
		// fmt.Println("Visited:", visited)
	}
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
func processLinks(htmlNode *html.Node, url string, urls *[]string, queuedUrls map[string]bool) {
	// find each link and add it to urls to process
	if htmlNode.Type == html.ElementNode && htmlNode.Data == "a" {
		for _, attr := range htmlNode.Attr {
			if attr.Key == "href" {
				// get absolute url
				link := strings.TrimSpace(attr.Val)
				link = resolveLink(url, link)

				// add link to urls
				if link != "" && !queuedUrls[link]{
					*urls = append(*urls, link)
					queuedUrls[link] = true
				}
			}
		}
	}

	// recursively process links
	for childNode := htmlNode.FirstChild; childNode != nil; childNode = childNode.NextSibling {
		processLinks(childNode, url, urls, queuedUrls)
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

/* logs url to LOGFILE */
func logUrl(url string, log *bufio.Writer) {
	_, err := log.WriteString(url + "\n")
	if err != nil {
		panic(err)
	}
}
