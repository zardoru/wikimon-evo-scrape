import sqlite3
import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://wikimon.net"
START = "/Category:List_of_Cards"
DB_FILE = "../../digi.db"

# Database connection
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
# Ensure foreign keys are enabled if needed, though not strictly used in the logic provided
conn.execute("PRAGMA foreign_keys = ON")

def make_refs(url, is_first_page):
    print(url)

    req = requests.get(url)
    soup = BeautifulSoup(req.content, 'html.parser')
    links = [(link.get("href"),) for link in soup.select(".mw-category-group a")]
    # find a link that says "next page" inside the tag text
    page_links = soup.select("a[title='Category:List of Cards']")

    conn.executemany("insert into refs(id, url, is_card) values (NULL, ?, 1) on conflict DO NOTHING", links)
    conn.commit()

    if len(page_links) == 4:
        return page_links[1].get("href")
    else:
        if not is_first_page:
            return None
        elif len(page_links) > 0:
            return page_links[0].get("href")

        return None


def fetch_card_links():
    next_page_link = make_refs(BASE_URL + START, True)
    while next_page_link:
        _next_page_link = make_refs(BASE_URL + next_page_link, False)
        next_page_link = _next_page_link

if __name__ == "__main__":
    fetch_card_links()