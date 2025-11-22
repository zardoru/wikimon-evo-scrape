import networkx as nx
import sqlite3 as sql
import json

db = sql.connect('digi.db')
G = nx.Graph()

cur = db.cursor()

digimon = ([{
    "id": x[0],
    "name": x[1],
    "previous": x[2] and json.loads(x[2]) or [],
    "next": x[3] and json.loads(x[3]) or []
} for x in cur
.execute('select id, name, previous, next, url, stage, attribute from digimon')
.fetchall()])

# print(json.dumps(digimon, indent=4))

for d in digimon:
    G.add_node(d['id'], name=d['name'], stage=d['stage'], attribute=d['attribute'])

for d in digimon:
    previous_connections = [(d['id'], item) for item in d['previous']]
    G.add_edges_from(previous_connections)

nx.write_graphml(G, 'digimon.graphml')