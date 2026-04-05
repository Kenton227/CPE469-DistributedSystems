## Group Members:
- Kenton Rhoden
- Sam Phan
## Usage:
```
> go run ./crawler.go <complete_urls> ...
```
**Optional Files**
- `stopWords.txt` - stop words to be filtered out separated by new lines
**Output Files**:
- `invIndex.json` - Stores the inverted index mapping keywords to associated URLs
- `visitedUrls.txt` - Stores all visited URLs
*Example usage*
```
> go run ./crawler.go https://books.toscrape.com/ https://calpoly.edu
```
## Notes:
- the default max number of pages to crawl is 100, set by the `MAX_PAGES` global constant
