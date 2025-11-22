wikimon digimon scraper
=====

This repository contains the code for scraping Wikimon for evolution lines and some other digimon data.
The scraping process not particularly user-friendly since using the different modes of operation in main.py requires choosing the right method to run.

The code is written in Python 3. Obviously, you will want to create a venv and install the requirements.

The files' use is as follows, in rough usage order:

* evodb.sql: Generates the digi.db file used by the other scripts. Use `sqlite3 digi.db < evodb.sql` to generate the file.
* fetch_cards_list.py: Gets the list of cards from Wikimon. It keeps you from nabbing about 10k cards data into your database. Used as a reference for deciding which evolutions are considered for the evolution line.
* main.py: Runs the scraping process. Has three modes of operation: Start from scratch, resume from pending links and reprocess html documents to update the evolutionary data.
* numberify.sql: Convert the evolution line links into lists of digimon IDs. It's a requirement to run the other scripts.

Once the main fetching procress is done, you can run the following scripts:
* render_line.py: Generates the evolution line graphs. It can receive an argument to specify the evolution line to render. It renders to graphml. Use `python render_line.py evolution_line_name` to render a specific evolution line. 
* to_graphml.py: Converts the digimon evolution data into graphml format. The resulting graph is extremely large, and the only way to view it is to use a tool like yEd or any similar graph viewer that has a neighborhood view and a search function.