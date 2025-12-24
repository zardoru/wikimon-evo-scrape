import argparse
import functools
import os
import sqlite3
import json
import time
from sys import stderr
from urllib.parse import urlencode, quote

import requests
from bs4 import BeautifulSoup
import lxml

from wikimonscrape.digidb import DigiDB, ScrapeDigimon

# Configuration
BASE_URL = "https://wikimon.net"
START = "/Reptilimon"

ASSUME_CARDS_FILLED = False
IGNORE_CARD_ONLY_REFS = True
LOW_EVO_COUNT = 3
# Minimum references will be set by command line argument

ddb = DigiDB()


def delayed_request(href):
    # cache hack
    html = ddb.get_digimon_html(href)
    if html:
        return html

    print(f"connecting to {href} no cache... ")
    time.sleep(1.5)
    url = BASE_URL + href
    return requests.get(url).text


def create_ref(href):
    req = delayed_request(href)
    soup = BeautifulSoup(req, 'lxml')

    # Check if 'Category:List of Cards' exists in the page
    is_card = bool(soup.select("a[title='Category:List of Cards']"))

    ddb.create_ref(href, req, is_card)
    return True, is_card


def is_cardgame_ref(href):
    url = quote(href)
    exists, is_card_game = ddb.get_ref(url, href)

    if exists:
        return is_card_game

    if ASSUME_CARDS_FILLED:
        return False

    _, is_card = create_ref(href)
    return is_card


def get_ref_data(item_tag, citation_area):
    """
    Extracts reference data from a given item tag and citation area.

    This function identifies references related to a citation item and determines whether they are
    exclusive to card games or include other types of references. It traverses the parent elements
    to find the relevant list item and processes its citations. The function checks whether these
    citations point to links specific to card games or include non-card game resources. The results
    include the determination of whether all references are for card games, a count of non-card game
    references, and the total number of citations.

    :param item_tag: The BeautifulSoup tag object identifying the starting citation point.
    :type item_tag: bs4.element.Tag
    :param citation_area: A list of BeautifulSoup elements representing the area where citation data
        is located.
    :type citation_area: list[bs4.element.Tag]
    :return: A tuple containing a boolean indicating if all references are card game-related, an
        integer count of non-card game references, and the total number of references.
    :rtype: tuple[bool, int, int]
    """

    # In BS4, next_sibling can be a Tag or NavigableString (text)
    parent = item_tag.parent
    while parent.name != "li":
        parent = parent.parent

    child_linkset = [x.get('href') for x in parent.select("a")
                     if x is not None and
                     x.get("href") is not None and
                     x.get("href").startswith("#cite")]
    is_only_cardgame_refs = True
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
                is_only_cardgame_refs = False
                noncard_ref_count += 1
        except AttributeError:
            print(f"citation {citation_href} failure", file=stderr)

    return is_only_cardgame_refs, noncard_ref_count, total_refs


def extract_evolutions(soup) -> tuple[list[str], list[str]]:
    """
    Extracts previous and next evolutions of an entity based on its web page content.

    The function parses the provided HTML content represented as a BeautifulSoup object
    to extract links that correspond to the entity's evolutionary relationships,
    specifically the "Evolves from" and "Evolves to" sections. It ensures that only
    valid links with significant references are retained.

    :param soup: BeautifulSoup
        The parsed HTML content of a web page.

    :return: tuple[list[str], list[str]]
        A tuple containing two lists:
        - The first list contains links (strings) to the previous evolutions.
        - The second list contains links (strings) to the next evolutions.
    """
    # Select 'li a[title]' and 'h2' in document order
    items = soup.select("li a[title]:first-of-type, h2")

    prev_evos_ref = []
    next_evos_ref = []
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

        if mode == 'prev':
            prev_evos_ref.append((href, noncard_ref_cnt))
        elif mode == 'next':
            next_evos_ref.append((href, noncard_ref_cnt))

    # Now, if the number of prev/next sibling evolutions is low enough, bypass minimum references:
    prev_evos = [x[0] for x in prev_evos_ref if
                 x[1] >= MIN_REFERENCES or
                 len(prev_evos_ref) <= LOW_EVO_COUNT]

    next_evos = [x[0] for x in next_evos_ref if
                 x[1] >= MIN_REFERENCES or
                 len(next_evos_ref) <= LOW_EVO_COUNT]

    if len(prev_evos) == 0 and len(next_evos) == 0:
        print(f"no evo links for {soup.select_one('h1').get_text().strip()}!")
        if len(prev_evos_ref) > 0 or len(next_evos_ref) > 0:
            print(f"prev refs: {prev_evos_ref}, next refs: {next_evos_ref},"
                  f" {len(prev_evos_ref)} prev, {len(next_evos_ref)}")

    return prev_evos, next_evos


def register_evo_links(digimon: ScrapeDigimon):
    ddb.register_evolution_links(digimon['prev_links'], digimon['next_links'], digimon['id'])


def get_evo_links(site):
    if not site.startswith("/"):
        return None

    if ddb.scraped(site):
        return ddb.digimon_by_site(site)

    print(f"getting {site}'s links ... ")
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

    digimon: ScrapeDigimon = {
        'id': None,
        'name': title,
        'url': site,
        'scraped': False,
        'prev_links': [],
        'next_links': [],
        'html': response
    }

    digimon = ddb.register_digimon(digimon)
    prev, nxt = extract_evolutions(soup)

    digimon['prev_links'] = prev
    digimon['next_links'] = nxt
    register_evo_links(digimon)

    if not prev and not nxt:
        print(f"no evo lines for {title}?")

    print(f"done with {digimon['name']}!")

    return digimon


def recurse_search(start_site):
    sites = [start_site]
    while sites:
        site = sites.pop()
        if ddb.scraped(site):
            digimon = ddb.digimon_by_site(site)
            if not digimon:
                continue

            # Ruby logic: add all links to stack if not scraped
            for link in (digimon['prev_links'] + digimon['next_links']):
                if not ddb.scraped(link):
                    sites.append(link)
            continue
        else:
            digimon = get_evo_links(site)
            ddb.mark_scraped(site)

        if not digimon:
            continue

        for link in digimon['prev_links']:
            if not ddb.scraped(link):
                sites.append(link)

        for link in digimon['next_links']:
            if not ddb.scraped(link):
                sites.append(link)


def resume_scrape():
    # SQL query to find unscraped links.
    # Note: Requires SQLite JSON1 extension (standard in modern Python/SQLite).

    try:
        for row in ddb.get_unscraped_links():
            recurse_search(row)
    except sqlite3.OperationalError as e:
        print(f"Error running resume_scrape query (likely missing JSON1 extension): {e}")


def refill_links():
    for row in ddb.get_digimon_urls_without_links():
        recurse_search(row)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Scrape Digimon evolution data from WikiMon.')
        parser.add_argument('--start-page', type=str, help='Starting page URL (e.g. /Reptilimon)', default=START)
        parser.add_argument('--resume', action='store_true', help='Resume scraping from unfinished links')
        parser.add_argument('--min-refs', type=int, default=2,
                            help='Minimum number of references required (default: 2)')
        parser.add_argument('--refill', action='store_true', help='Refill missing evolution links')
        parser.add_argument('--low-evo-threshold', type=int, default=3,
                            help='Count of evolutions at which Minimum number of References is ignored (default: 3)')
        args = parser.parse_args()

        print(args)

        MIN_REFERENCES = args.min_refs
        LOW_EVO_COUNT = args.low_evo_threshold

        print(f"Starting with {MIN_REFERENCES} minimum references and {LOW_EVO_COUNT} low evo threshold")
        print(f"Assuming cards are filled: {ASSUME_CARDS_FILLED}")
        print(f"Ignoring card-only references: {IGNORE_CARD_ONLY_REFS}")
        print(f"Current working directory: {os.getcwd()}")

        if args.resume:
            print("Resuming scraping...")
            resume_scrape()
        elif args.refill:
            print("Refilling missing links...")
            refill_links()
        else:
            print(f"Scraping from {BASE_URL}{args.start_page}...")
            recurse_search(args.start_page)
    except Exception as e:
        print(f"Error: {e}")
