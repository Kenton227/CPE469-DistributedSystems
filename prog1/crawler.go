package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"slices"
	"time"

	"bufio"
	"encoding/json"
	"strings"

	"github.com/PuerkitoBio/goquery"

	"regexp"
)

const MAX_PAGES = 100
const FIRST_MILESTONE = 100
const MILESTONE_GROWTH_FCTR = 10
const LOGFILE = "visitedUrls.txt"
const STOPWORDSFILE = "stopWords.txt"
const JSONFILE = "invIndex.json"
const TEXT_ELEMENTS = "title, h1, h2, h3, h4, h5, h6, p, li, td, th, blockquote, pre, a"

var nonAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)

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

	urls := os.Args[1:]
	processUrls(urls, invIndex, stopWords, log, startTime)

	saveJson(invIndex)

	printFirstKeywords(invIndex)
}

func printFirstKeywords(index map[string][]string) {
	// Print first 10 keywords in index (sorted by most related URLs)
	keywords := make([]string, 0, len(index))
	for k := range index {
		keywords = append(keywords, k)
	}
	slices.SortFunc(keywords, func(a, b string) int {
		if len(index[a]) < len(index[b]) {
			return -1
		}
		if len(index[a]) < len(index[b]) {
			return 1
		}
		return 0
	})
	for i := range min(10, len(keywords)) {
		fmt.Printf("\t%d: %s\n", i+1, keywords[i])
	}
}

/* Returns map of stop words from STOPWORDSFILE or empty map with error on error. */
func getStopWords() map[string]bool {

	stopWords := make(map[string]bool)
	fptr, err := os.Open(STOPWORDSFILE)
	if err != nil {
		fmt.Println("Warning:", err)
		fmt.Println("Continuing with no filtered stop words...")
		return stopWords
	}
	defer fptr.Close()

	scanner := bufio.NewScanner(fptr)
	for scanner.Scan() {
		stopWords[scanner.Text()] = true
	}

	return stopWords
}

/* takes in invIndex map and writes it to JSONFILE */
func saveJson(invIndex map[string][]string) {
	// get JSON encoding of map
	jsonData, err := json.MarshalIndent(invIndex, "", "  ")
	if err != nil {
		panic(err)
	}

	fptr, err := os.Create(JSONFILE)
	if err != nil {
		panic(err)
	}
	defer fptr.Close()

	_, err = fptr.Write(jsonData)
	if err != nil {
		panic(err)
	}
}

/* stores each word from url into inverted index, updates visited */
func processUrls(
	urls []string,
	invIndex map[string][]string,
	stopWords map[string]bool,
	log *bufio.Writer,
	startTime time.Time,
) {

	/*Stores all URLs ever enqueued*/
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

		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("Request failed:", url, "-", err)
			continue
		}
		defer resp.Body.Close()

		document, err := goquery.NewDocumentFromReader(resp.Body)
		if err != nil {
			continue
		}

		// Process Documents
		processDocText(document, url, invIndex, stopWords)
		processDocLinks(document, url, &urls, queuedUrls)

		if visited == nextMilestone {
			fmt.Printf("Visited %d pages!\n", visited)
			printTimeSinceStart(startTime)
			nextMilestone *= MILESTONE_GROWTH_FCTR
		}

	}
}

func extractTextFromDoc(doc *goquery.Document) []string {
	doc.Find("script, style, noscript, svg").Remove()

	var tokens []string

	doc.Find(TEXT_ELEMENTS).Each(
		func(i int, s *goquery.Selection) {
			text := strings.TrimSpace(s.Text())
			cleanText := cleanText(text)
			for _, token := range strings.Fields(cleanText) {
				if token == "" {
					continue
				}
				tokens = append(tokens, token)
			}
		},
	)

	return tokens
}

func processDocText(doc *goquery.Document, url string, invIndex map[string][]string, stopWords map[string]bool) {
	words := extractTextFromDoc(doc)

	for _, word := range words {
		if !stopWords[word] {
			addToInvIndex(word, url, invIndex)
		}
	}
}

func isValidHTTP(link string) bool {
	u, err := url.Parse(link)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

func processDocLinks(doc *goquery.Document, url string, urls *[]string, queuedUrls map[string]bool) {
	doc.Find("a[href]").Each(
		func(i int, s *goquery.Selection) {
			link, exists := s.Attr("href")
			if exists {
				link = resolveLink(url, strings.TrimSpace(link))          // get absolute url
				if link != "" && isValidHTTP(link) && !queuedUrls[link] { // add link to urls
					*urls = append(*urls, link)
					queuedUrls[link] = true
				}
			}
		},
	)
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
		}
	}

	// add url to invIndex
	invIndex[word] = append(valueUrls, url)
}

/* Replaces non-alphanumeric characters with spaces */
func cleanText(s string) string {
	return nonAlphaNumeric.ReplaceAllString(strings.ToLower(s), " ")
}

/* logs url to LOGFILE */
func logUrl(url string, log *bufio.Writer) {
	_, err := log.WriteString(url + "\n")
	if err != nil {
		panic(err)
	}
}
