from typing import Set

import networkx as nx
import sqlite3 as sql
import json
import sys

db = sql.connect('digi.db')
G = nx.DiGraph()

line = len(sys.argv) > 1 and sys.argv[1] or "Examon"

cursor = db.cursor()

item = cursor.execute('''select id, name, previous, next, stage
                         from digimon
                         where name =?''', (line,)).fetchone()


def digimon_by_id(id):
    return cursor.execute(
        '''select id, name, previous, next, stage
           from digimon
           where id=?''', (id,)).fetchone()


def recursive_add(G, item, seen: Set, do_next=False, do_previous=False):
    if item[0] in seen:
        return

    stage = item[4]

    G.add_node(item[0], name=item[1])
    seen.add(item[0])

    prev = item[2] and json.loads(item[2]) or []
    next_evo = item[3] and json.loads(item[3]) or []
    if do_previous:
        for prev_id in prev:
            prev_digimon = digimon_by_id(prev_id)

            if prev_digimon and (prev_digimon[4] or -1) <= (stage or -1):
                recursive_add(G, prev_digimon, seen, do_next=False, do_previous=True)
                G.add_edge(prev_id, item[0])

            print(f"prev: {prev_digimon[1]} -> {item[1]}")

    if do_next:
        for next_id in next_evo:
            next_digimon = digimon_by_id(next_id)

            if next_digimon and (next_digimon[4] or -1) >= (stage or -1):
                recursive_add(G, next_digimon, seen, do_next=True, do_previous=False)
                G.add_edge(item[0], next_id)

            print(f"next: {item[1]} -> {next_digimon[1]}")


recursive_add(G, item, set(), True, True)

nx.write_graphml(G, f"{line}.graphml")
