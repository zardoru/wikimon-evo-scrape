import functools
import sqlite3
import json
import time
from sys import stderr
from urllib.parse import urlencode, quote

import requests
from bs4 import BeautifulSoup
import lxml

# Configuration
BASE_URL = "https://wikimon.net"
START = "/DORUmon"
DB_FILE = "digi.db"

ASSUME_CARDS_FILLED = False
IGNORE_CARD_ONLY_REFS = True
MIN_REFERENCES = 2

# Database connection
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
# Ensure foreign keys are enabled if needed, though not strictly used in the logic provided
conn.execute("PRAGMA foreign_keys = ON")


def delayed_request(href):
    # cache hack
    cur = conn.execute("select html from digimon where url=? and html is not null", (href,))
    row = cur.fetchone()
    if row:
        return row[0]

    print(f"connecting to {href} no cache... ")
    time.sleep(1.5)
    url = BASE_URL + href
    return requests.get(url).text


def exists_digimon(digimon_name):
    cur = conn.execute("select id from digimon where name = ?", (digimon_name,))
    row = cur.fetchone()
    return row[0] if row else None


def register_digimon(digimon):
    existing_id = exists_digimon(digimon['name'])
    if existing_id is not None:
        result = {'id': existing_id}
        result.update(digimon)
        return result

    # Using lastrowid for broad compatibility
    cur = conn.execute(
        "insert into digimon(id, name, url, html) values (NULL, ?, ?, ?)",
        (digimon['name'], digimon['url'], digimon['html'])
    )
    conn.commit()
    new_id = cur.lastrowid

    result = {'id': new_id}
    result.update(digimon)
    return result


def register_evolutions(previous_stage, next_stage, digimon_id):
    conn.execute(
        "update digimon set previous=?, next=? where id=?",
        (json.dumps(previous_stage), json.dumps(next_stage), digimon_id)
    )
    conn.commit()


def get_ref(href, href2):
    cur = conn.execute("select 1, is_card from refs where url=? or url=?", (href, href2))
    row = cur.fetchone()
    if not row:
        return False, False
    return True, bool(row[1])


def create_ref(href):
    req = delayed_request(href)
    soup = BeautifulSoup(req, 'lxml')

    # Check if 'Category:List of Cards' exists in the page
    is_card = bool(soup.select("a[title='Category:List of Cards']"))

    conn.execute(
        "insert into refs(id, url, html, is_card) values (NULL, ?, ?, ?) on conflict do nothing ",
        (href, req, is_card)
    )
    conn.commit()

    return True, is_card


def is_cardgame_ref(href):
    url = quote(href)
    exists, is_card_game = get_ref(url, href)

    if exists:
        return is_card_game

    if ASSUME_CARDS_FILLED:
        return False

    _, is_card = create_ref(href)
    return is_card


def get_ref_data(item_tag, citation_area):
    """
    Checks if all sibling references of an item are card game references.
    Mimics the Ruby logic: iterates over next siblings.
    """

    # In BS4, next_sibling can be a Tag or NavigableString (text)
    parent = item_tag.parent
    while parent.name != "li":
        parent = parent.parent

    child_linkset = [x.get('href') for x in parent.select("a")
                     if x is not None and
                     x.get("href") is not None and
                     x.get("href").startswith("#cite")]
    only_cardgame_refs = True
    noncard_ref_count = 0
    total_refs = len(child_linkset)

    for citation_href in child_linkset:

        # find the node corresponding to this citation
        citation_area_links = [x.select_one(f"[id='{citation_href.lstrip("#")}'] .reference-text a") for x in
                               citation_area]
        found_citations = [x for x in citation_area_links if x is not None]
        if len(found_citations) == 0:
            print(f"no href for {citation_href}")
            continue

        real_href = found_citations[0]

        # get the links
        real_href = real_href.get('href')

        # battle-spirits are all card refs
        if "battle-spirits" in real_href.lower():
            continue

        # they're not wikimon refs? we skip em
        if not real_href.startswith("/"):
            print(f"non-local wikimon ref {real_href}")
            continue

        try:
            if not is_cardgame_ref(real_href):
                only_cardgame_refs = False
                noncard_ref_count += 1
        except AttributeError:
            print(f"citation {citation_href} failure", file=stderr)

    return only_cardgame_refs, noncard_ref_count, total_refs


def extract_evolutions(soup):
    # Select 'li a[title]' and 'h2' in document order
    items = soup.select("li a[title]:first-of-type, h2")

    prev_evos = []
    next_evos = []
    mode = 'whatever'

    citation_area = soup.select(".references")

    for item in items:
        node_type = item.name
        node_text = item.get_text().strip().lower()

        if node_type == 'h2':
            if "evolves from" in node_text:
                mode = 'prev'
                continue
            elif "evolves to" in node_text:
                mode = 'next'
                continue
            elif mode == 'next':
                # New section after 'next', we gotta leave
                break

        if mode == 'whatever' or node_type != 'a':
            continue

        href = item.get('href')
        if "card game" in node_text:
            continue

        is_cardgame_ref_only, noncard_ref_cnt, ref_total = get_ref_data(item, citation_area)
        if is_cardgame_ref_only and IGNORE_CARD_ONLY_REFS:
            continue

        if noncard_ref_cnt < MIN_REFERENCES:
            continue

        if mode == 'prev':
            prev_evos.append(href)
        elif mode == 'next':
            next_evos.append(href)

    return prev_evos, next_evos


def scraped(site):
    query = """
            select 1
            from scraped
            where site = ?
              and site not in (select url from digimon where url = ? and scraped = 0)
            union all
            select 1
            from digimon
            where url = ?
              and scraped = 1 \
            """
    cur = conn.execute(query, (site, site, site))
    return cur.fetchone() is not None


def mark_scraped(site):
    conn.execute("insert into scraped values (?)", (site,))
    conn.commit()


def digimon_by_site(site):
    cur = conn.execute(
        "select id, name, url, scraped, prev_links, next_links from digimon where url=?",
        (site,)
    )
    data = cur.fetchone()
    if not data:
        return None

    return {
        'id': data[0],
        'name': data[1],
        'url': data[2],
        'scraped': data[3],
        'prev_links': json.loads(data[4]) if data[4] else [],
        'next_links': json.loads(data[5]) if data[5] else []
    }


def register_evo_links(prev, nxt, digimon):
    conn.execute(
        "update digimon set prev_links=?, next_links=? where id=?",
        (json.dumps(prev), json.dumps(nxt), digimon['id'])
    )
    conn.commit()

    digimon['prev_links'] = prev
    digimon['next_links'] = nxt
    return digimon


def get_evo_links(site):
    if not site.startswith("/"):
        return None

    if scraped(site):
        return digimon_by_site(site)

    print(f"getting {site}'s links ... ")
    # Fixed bug from original script: href was undefined, meant to be site
    response = delayed_request(site)
    soup = BeautifulSoup(response, 'lxml')

    print(f"processing document for {site} ")

    if not soup.select("#catlinks a[title='Category:Digimon']"):
        # it ain't a digimon lol
        return None

    # Extract title
    # Ruby: text.strip.reverse.chomp("/").reverse
    raw_title = soup.select_one("#firstHeading").get_text().strip()
    if raw_title.endswith("/"):
        title = raw_title[:-1]
    else:
        title = raw_title

    digimon_data = {
        'name': title,
        'url': site,
        'html': response
    }
    digimon = register_digimon(digimon_data)

    prev, nxt = extract_evolutions(soup)
    digimon = register_evo_links(prev, nxt, digimon)

    if not prev and not nxt:
        print(f"no evo lines for {title}?")

    print(f"done with {digimon['name']}!")

    return digimon


def recurse_search(start_site):
    sites = [start_site]
    while sites:
        site = sites.pop()
        if scraped(site):
            digimon = digimon_by_site(site)
            if not digimon:
                continue

            # Ruby logic: add all links to stack if not scraped
            for link in (digimon['prev_links'] + digimon['next_links']):
                if not scraped(link):
                    sites.append(link)
            continue
        else:
            digimon = get_evo_links(site)
            mark_scraped(site)

        if not digimon:
            continue

        for link in digimon['prev_links']:
            if not scraped(link):
                sites.append(link)

        for link in digimon['next_links']:
            if not scraped(link):
                sites.append(link)


def resume_scrape():
    # SQL query to find unscraped links.
    # Note: Requires SQLite JSON1 extension (standard in modern Python/SQLite).
    query = """
            with all_links as (select distinct site
                               from (select json_each.value as site
                                     from digimon, json_each(prev_links)
                                     union all
                                     select json_each.value as site
                                     from digimon, json_each(next_links))),
                 all_scrapes as (select scraped.site
                                 from scraped
                                 union all
                                 select digimon.url
                                 from digimon
                                 where scraped = 1)
            select *
            from all_links
            where site not in all_scrapes \
            """
    try:
        cur = conn.execute(query)
        rows = cur.fetchall()
        for row in rows:
            recurse_search(row[0])
    except sqlite3.OperationalError as e:
        print(f"Error running resume_scrape query (likely missing JSON1 extension): {e}")

def refill_links():
    query = """select id, url from digimon where prev_links is null and next_links is null"""

    cur = conn.execute(query)
    rows = cur.fetchall()
    for row in rows:
        recurse_search(row[1])

if __name__ == "__main__":
    try:
        # recurse_search(START)
        # resume_scrape()
        refill_links()
    finally:
        conn.close()
