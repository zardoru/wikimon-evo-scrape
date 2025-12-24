drop table if exists digimon;
create table digimon(
	id integer primary key,
	name text not null,
	previous text,
	next text,
    prev_links text,
    next_links text,
	attribute text,
	url text,
    html text,
    stage int,
    type text,
    scraped boolean generated always as (prev_links is not null and next_links is not null)
);

create index ix_url on digimon(url);

drop table if exists scraped;
create table scraped(
    site
);

create index ix_scraped on scraped(site);

drop table refs;
create table refs(
    id integer primary key,
    url text not null,
    html text,
    is_card boolean
);

create unique index ix_ref_url on refs(url);