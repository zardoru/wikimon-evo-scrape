import dataclasses
import json
import sqlite3 as sql
from typing import TypedDict

base_query = '''
             select id, name, previous, next, stage, attribute, url
             from digimon
             '''

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

display_stage = {
    1: "Baby I",
    2: "Baby II",
    3: "Child",
    4: "Adult",
    5: "Perfect",
    6: "Ultimate"
}


@dataclasses.dataclass
class Digimon:
    """
    Represents a Digimon entity with its key information, such as ID, name, evolutionary stages,
    attribute type, and related details.

    This class is designed to manage and store information about a Digimon. It includes its unique
    identifier, its name, the evolutionary stages connected to it (both previous and next), its current
    evolutionary stage, and its attribute type. Additionally, a URL can be associated with the Digimon for
    external reference if necessary.

    :ivar id: The unique identifier for the Digimon.
    :type id: int
    :ivar name: The name of the Digimon.
    :type name: str
    :ivar previous: A list of IDs representing the previous evolutionary stages of the Digimon.
    :type previous: list[int]
    :ivar next: A list of IDs representing the next evolutionary stages of the Digimon.
    :type next: list[int]
    :ivar stage: The evolutionary stage of the Digimon. Refer to display_stage.
    :type stage: int
    :ivar attribute: The attribute type of the Digimon, such as "Virus", "Data", or "Vaccine".
    :type attribute: str
    :ivar url: An optional URL providing additional information about the Digimon. It always starts with /, as it is a link.
    :type url: str | None
    """
    id: int
    name: str
    previous: list[int]
    next: list[int]
    stage: int
    attribute: str
    url: str | None = None


class ScrapeDigimon(TypedDict):
    id: int | None
    name: str
    url: str
    scraped: bool
    prev_links: list[str]
    next_links: list[str]
    html: str | None


class DigiDB:
    """
    Represents a database handler for managing and querying a Digimon database.

    This class provides methods to interact with a SQLite database containing Digimon
    data. It allows querying Digimon by name or ID, retrieving all Digimon records,
    and managing connections to the database.

    The *_raw methods return the database rows directly without any processing.

    :ivar db: SQLite connection object for the database file.
    :type db: sqlite3.Connection
    :ivar cursor: The cursor object for executing SQLite commands.
    :type cursor: sqlite3.Cursor
    """

    def __init__(self, fname="digi.db"):
        """
        Initializes a new instance of the database connection class.

        This constructor sets up a connection to the database file specified by the
        parameter and creates a cursor object for executing SQL queries.

        :param fname: The name of the database file to connect to. If not specified,
            defaults to 'digi.db'.
        :type fname: str
        """
        self.db = sql.connect(fname)
        self.cursor = self.db.cursor()

    def digimon_by_name_raw(self, name):
        return self.cursor.execute(
            f'''{base_query}
               where name=?''', (name,)).fetchone()

    def digimon_by_id_raw(self, id):
        return self.cursor.execute(
            f'''{base_query}
               where id=?''', (id,)).fetchone()

    def digimon_by_name(self, name):
        raw = self.digimon_by_name_raw(name)
        if raw is None:
            return None
        return self._raw_to_digimon(raw)

    def digimon_by_id(self, id):
        raw = self.digimon_by_id_raw(id)
        if raw is None:
            return None
        return self._raw_to_digimon(raw)

    @staticmethod
    def _raw_to_digimon(raw):
        return Digimon(
            id=raw[0],
            name=raw[1],
            previous=json.loads(raw[2]) if raw[2] else [],
            next=json.loads(raw[3]) if raw[3] else [],
            stage=raw[4],
            attribute=raw[5],
            url=raw[6]
        )

    def digimon_from_namelist(self, namelist):
        digimon_list = []
        for line in namelist:
            digimon = self.digimon_by_name(line)
            if digimon is None:
                print(f"not found: {line}")
                continue
            digimon_list.append(digimon)

        return digimon_list

    def all_digimon(self):
        return self.cursor.execute(base_query).fetchall()

    def scraped(self, site):
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
        cur = self.db.execute(query, (site, site, site))
        return cur.fetchone() is not None

    def mark_scraped(self, site):
        self.db.execute("insert into scraped values (?)", (site,))
        self.db.commit()

    def digimon_by_site(self, site) -> ScrapeDigimon | None:
        cur = self.db.execute(
            "select id, name, url, scraped, prev_links, next_links from digimon where url=?",
            (site,)
        )
        data = cur.fetchone()
        if not data:
            return None

        return ScrapeDigimon(
            id=data[0],
            name=data[1],
            url=data[2],
            scraped=data[3],
            prev_links=json.loads(data[4]) if data[4] else [],
            next_links=json.loads(data[5]) if data[5] else [],
            html=None
        )

    def create_ref(self, href: str, html: str, is_card: bool):
        """
        Inserts a new record into the `refs` table with the specified URL, HTML
        content, and card status. If a conflict occurs (e.g., duplicate unique
        constraint), the operation will be ignored.

        The `refs` table stores a list of URLs corresponding to references in the wiki page
        and their associated HTML content, alongside
        the is_card flag indicating whether the reference is a card.

        :param href: The URL reference to be inserted.
        :type href: str
        :param html: The HTML content associated with the reference.
        :type html: str
        :param is_card: A boolean flag indicating whether the reference is marked as
                        a card.
        :type is_card: bool
        :return: None
        """
        self.db.execute(
            "insert into refs(id, url, html, is_card) values (NULL, ?, ?, ?) on conflict do nothing ",
            (href, html, is_card)
        )
        self.db.commit()

    def get_ref(self, href, href2) -> tuple[bool, bool]:
        """
        Determines whether either of the provided URLs (`href` or `href2`) exists in the
        `refs` database and if so, identifies whether the corresponding entry is
        a card (`is_card`).

        This method queries the database for any record that matches either of the
        provided URLs. If no matching record is found, it returns a tuple indicating
        that there is no match and the `is_card` status cannot be determined. If a
        matching record is found, it returns a tuple where the first value is `True` to
        indicate a match, and the second value reflects the boolean interpretation of
        the `is_card` field retrieved from the database.

        :param href: The first URL to check for a match in the `refs` database.
        :type href: str
        :param href2: The second URL to check for a match in the `refs` database.
        :type href2: str
        :return: A tuple where the first element indicates whether a matching record
            was found (`True` or `False`) and the second element indicates the
            `is_card` status of the matched record (`True` or `False`).
        :rtype: tuple[bool, bool]
        """
        cur = self.db.execute("select 1, is_card from refs where url=? or url=?", (href, href2))
        row = cur.fetchone()
        if not row:
            return False, False
        return True, bool(row[1])

    def register_evolution_links(self, prev: list[str], nxt: list[str], id: int):
        """
        Registers evolution links for a Digimon in the database.

        This function updates the `prev_links` and `next_links` fields for a Digimon
        in the database, based on its `id`. The `prev_links` and `next_links` are
        stored as JSON strings in the database. After updating, the changes are
        committed to the database.

        :param prev: A list of strings representing the previous evolution links
            of the Digimon.
        :param nxt: A list of strings representing the next evolution links
            of the Digimon.
        :param id: The unique identifier of the Digimon.
        :return: None
        """
        self.db.execute(
            "update digimon set prev_links=?, next_links=? where id=?",
            (json.dumps(prev), json.dumps(nxt), id)
        )
        self.db.commit()

    def get_digimon_html(self, url: str) -> str | None:
        """
        Fetches the stored HTML content for a given Digimon URL from the database if it exists.

        This method queries the database to retrieve the HTML content associated with
        a specific Digimon URL, provided the content is not null.

        :param url: The URL of the Digimon entry to look up in the database.
        :type url: str
        :return: The stored HTML content as a string if found, or None if the entry does
                 not exist or has no HTML content.
        :rtype: str | None
        """
        cur = self.db.execute("select html from digimon where url=? and html is not null", (url,))
        row = cur.fetchone()
        return row[0] if row else None

    def exists_digimon(self, digimon_name) -> int | None:
        """
        Checks if a Digimon exists in the database by its name and retrieves its ID
        if present.

        :param digimon_name: The name of the Digimon to look for.
        :type digimon_name: str
        :return: The ID of the Digimon if it exists, otherwise None.
        :rtype: int | None
        """
        cur = self.db.execute("select id from digimon where name = ?", (digimon_name,))
        row = cur.fetchone()
        return row[0] if row else None

    def register_digimon(self, digimon: ScrapeDigimon) -> ScrapeDigimon:
        existing_id = self.exists_digimon(digimon['name'])
        if existing_id is not None:
            result = digimon.copy()
            result['id'] = existing_id
            return result

        # Using lastrowid for broad compatibility
        cur = self.db.execute(
            "insert into digimon(id, name, url, html) values (NULL, ?, ?, ?)",
            (digimon['name'], digimon['url'], digimon['html'])
        )
        self.db.commit()
        new_id = cur.lastrowid

        result = digimon.copy()
        result['id'] = new_id
        return result

    def get_unscraped_links(self):
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

        cur = self.db.execute(query)
        rows = cur.fetchall()
        return [row[0] for row in rows]

    def get_digimon_urls_without_links(self):
        """
        Retrieve all URLs from the "digimon" table where both 'prev_links' and 'next_links' columns are null.
        This will return all the URLs of digimon who have not had their evolution links scraped yet.

        :param self: The instance of the class calling the method.
        :return: A list of URLs as strings associated with Digimon records that satisfy the query condition.
        :rtype: List[str]
        """
        query = """select id, url \
                   from digimon \
                   where prev_links is null \
                     and next_links is null"""

        cur = self.db.execute(query)
        rows = cur.fetchall()
        return [row[1] for row in rows]

    def close(self):
        self.db.close()


def group_by_stage(digimon_list):
    stages = {}
    for digimon in digimon_list:
        stages.setdefault(digimon.stage, []).append(digimon)

    return stages
