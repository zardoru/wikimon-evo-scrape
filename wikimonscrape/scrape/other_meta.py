import os
import sqlite3 as sql
from tqdm import tqdm

from bs4 import BeautifulSoup

db = sql.connect('digi.db')

def parent_td(node):
    ret = node
    while ret.name != "td":
        ret = ret.parent

    return ret

def adjacent_td(node):
    return parent_td(node).find_next_sibling("td")

stage_map = {
    "baby i": 1,
    "baby ii": 2,
    "child": 3,
    "adult": 4,
    "perfect": 5,
    "ultimate": 6,
    "armor": 4,
    "hybrid": 4
}

def get_stage(soap):
    node = soap.select_one("a[title='Evolution Stage']")
    if not node:
        return -1

    adjacent = adjacent_td(node)
    text = adjacent.text.lower().strip()

    if text not in stage_map:
        return -1

    return stage_map[text]

def get_type(soap):
    node = soap.select_one("a[title='Type']")
    if not node:
        return ""

    adjacent = adjacent_td(node)
    return adjacent and adjacent.text or ""

def get_attribute(soap):
    node = soap.select_one("a[title='Attribute']")
    if not node:
        return ""

    adjacent = adjacent_td(node)
    return adjacent and adjacent.text or ""

def update_all():
    digilist = db.execute("select id, html from digimon where html is not null and attribute is null").fetchall()
    updates = []

    for digimon in tqdm(digilist):
        soap = BeautifulSoup(digimon[1], 'lxml')
        updates.append(( get_stage(soap), get_type(soap), get_attribute(soap), digimon[0]))

    db.executemany("update digimon set stage=?, type=?, attribute=? where id=?", updates)
    db.commit()

if  __name__ == "__main__":
    print("updating digimon attributes ...")
    print(f"cwd: {os.getcwd()}")
    update_all()
