## Group Members:
* Kenton Rhoden
* Sam Phan
## Usage:
```
> ./crawler.exe <complete_urls> ...
```
*Output Files*:
* `invIndex.json` - Stores the inverted index mapping keywords to associated URLs
* `visitedUrls.txt` - Stores all visited URLs
* `crawler.exe` - Executable program 
(Bash Example):
```
> go build
> ./crawler.exe https://books.toscrape.com/ https://calpoly.edu
```