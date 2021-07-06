# Simple Web Scraper - Investigative Journalism Foundation Technical Test
## Nova Scotia Lobbyist Data
## 2021-07-05


## Notes
-	Selenium doesn’t work
    -   The lobbyist profiles are delivered through ASP, so will likely require either Selenium or more advanced requests/urllib calls
-	How to check if page has been updated with new names (tracking names? Webpage changes?)
    -	Tracking names will require method of avoiding completely duplicate entries
    -	Page has a Total Records listing, comparison with existing file will likely be way forward, then do a simple substraction
-   Some of the requests don't work, most likely due to encoding issues
-   The rest of the provinces
