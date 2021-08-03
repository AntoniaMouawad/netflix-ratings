import configparser

config = configparser.ConfigParser()
config.read('config.cfg')

create_staging_imdb_titles = """CREATE TABLE IF NOT EXISTS staging_imdb_titles(
tconst VARCHAR NOT NULL PRIMARY KEY,
titleType VARCHAR,
primaryTitle VARCHAR,
originalTitle VARCHAR,
isAdult VARCHAR,
startYear VARCHAR,
endYear VARCHAR,
runtimeMinutes VARCHAR,
genres VARCHAR);
"""

create_staging_imdb_ratings = """CREATE TABLE IF NOT EXISTS staging_imdb_ratings(
tconst VARCHAR NOT NULL,
averageRating FLOAT,
numVotes INT);
"""

create_staging_imdb_principals = """CREATE TABLE IF NOT EXISTS staging_imdb_principals(
tconst VARCHAR NOT NULL,
ordering INTEGER NOT NULL,
nconst VARCHAR NOT NULL, 
category VARCHAR NOT NULL,
job VARCHAR NOT NULL,
characters VARCHAR NOT NULL);
"""

create_staging_imdb_crew = """CREATE TABLE IF NOT EXISTS staging_imdb_crew(
tconst VARCHAR NOT NULL,
directors VARCHAR, 
writers VARCHAR);
"""

create_staging_imdb_names = """CREATE TABLE IF NOT EXISTS staging_imdb_names(
nconst VARCHAR NOT NULL PRIMARY KEY,
primaryName VARCHAR NOT NULL,
birthYear VARCHAR,
deathYear VARCHAR,
primaryProfession VARCHAR,
knownForTitles VARCHAR);
"""

create_staging_rotten_tomatoes_titles = """CREATE TABLE IF NOT EXISTS staging_rotten_tomatoes_titles(
rotten_tomatoes_link VARCHAR, 
movie_title VARCHAR,
movie_info VARCHAR,
critics_consensus VARCHAR, 
content_rating VARCHAR, 
genres VARCHAR, 
directors VARCHAR, 
authors VARCHAR,
actors VARCHAR, 
original_release_date DATE, 
streaming_release_date DATE, 
runtime FLOAT,
production_company VARCHAR, 
tomatometer_status VARCHAR, 
tomatometer_rating FLOAT,
tomatometer_count INT, 
audience_status VARCHAR, 
audience_rating FLOAT,
audience_count INT, 
tomatometer_top_critics_count INT,
tomatometer_fresh_critics_count INT, 
tomatometer_rotten_critics_count INT);
"""
create_staging_netflix_titles = """CREATE TABLE IF NOT EXISTS staging_netflix_titles(
show_id VARCHAR NOT NULL PRIMARY KEY,
"type" VARCHAR,
title VARCHAR,
director VARCHAR,
"cast" VARCHAR,
country VARCHAR,
date_added DATE,
release_year INT4,
rating VARCHAR,
duration VARCHAR,
listed_in VARCHAR,
description VARCHAR);
"""
create_titles = \
    """CREATE TABLE IF NOT EXISTS titles(
      id SERIAL PRIMARY KEY,
      netflix_id VARCHAR NOT NULL,
      imdb_id VARCHAR,
      rt_id VARCHAR,
      type VARCHAR,
      name VARCHAR,
      normalized_name VARCHAR,
      netflix_description VARCHAR,
      release_year INT4,
      runtime_minutes INT,
      country VARCHAR,
      rated VARCHAR,
      imdb_avg_score FLOAT,
      imdb_n_ratings INT,
      rt_critics_score FLOAT,
      rt_n_critics INT,
      rt_audience_score FLOAT,
      rt_n_audience INT,
      rt_critics_consensus VARCHAR);
    """

create_roles = \
    """CREATE TABLE IF NOT EXISTS roles(
    id SERIAL PRIMARY KEY,
    title_id VARCHAR,
    person_id VARCHAR,
    role_name VARCHAR NOT NULL
    );
    """

create_persons = \
    """CREATE TABLE IF NOT EXISTS persons(
    id VARCHAR NOT NULL PRIMARY KEY,
    full_name VARCHAR NOT NULL,
    birth_year INT4,
    death_year INT4
    );
    """

create_genres = \
    """CREATE TABLE IF NOT EXISTS genres(
    id SERIAL PRIMARY KEY,
    title_id VARCHAR NOT NULL,
    genre_name VARCHAR NOT NULL);"""

create_table_queries = [create_staging_imdb_titles, create_staging_imdb_ratings, create_staging_imdb_principals,
                        create_staging_imdb_names, create_staging_rotten_tomatoes_titles, create_staging_netflix_titles,
                        create_staging_imdb_crew, create_titles, create_roles, create_persons, create_genres]

drop_staging_imdb_titles = """DROP TABLE IF EXISTS staging_imdb_titles"""
drop_staging_imdb_ratings = """DROP TABLE IF EXISTS staging_imdb_ratings"""
drop_staging_imdb_principals = """DROP TABLE IF EXISTS staging_imdb_principals"""
drop_staging_imdb_crew = """DROP TABLE IF EXISTS staging_imdb_crew"""
drop_staging_imdb_names = """DROP TABLE IF EXISTS staging_imdb_names"""
drop_staging_rotten_tomatoes_titles = """DROP TABLE IF EXISTS staging_rotten_tomatoes_titles"""
drop_staging_netflix_titles = """DROP TABLE IF EXISTS staging_netflix_titles"""
drop_titles = """DROP TABLE IF EXISTS titles"""
drop_roles = """DROP TABLE IF EXISTS roles"""
drop_persons = """DROP TABLE IF EXISTS persons"""
drop_genres = """DROP TABLE IF EXISTS genres"""

drop_table_queries = [drop_staging_imdb_titles, drop_staging_imdb_ratings, drop_staging_imdb_principals,
                      drop_staging_imdb_crew, drop_staging_imdb_names, drop_staging_rotten_tomatoes_titles,
                      drop_staging_netflix_titles, drop_titles, drop_roles, drop_persons, drop_genres]

copy_staging_titles = f"COPY staging_imdb_titles FROM {config['DATA_PATH']['imdb_titles_path']} delimiter '\t' CSV HEADER"
copy_staging_ratings = f"COPY staging_imdb_ratings FROM {config['DATA_PATH']['imdb_ratings_path']} delimiter '\t' CSV HEADER"
copy_staging_principals = f"COPY staging_imdb_principals FROM {config['DATA_PATH']['imdb_principals_path']} delimiter '\t' CSV HEADER"
copy_staging_crew = f"COPY staging_imdb_crew FROM {config['DATA_PATH']['imdb_crew_path']} delimiter '\t' CSV HEADER"
copy_staging_names = f"COPY staging_imdb_names FROM {config['DATA_PATH']['imdb_names_path']} delimiter '\t' CSV HEADER"
copy_staging_rt = f"COPY staging_rotten_tomatoes_titles FROM {config['DATA_PATH']['rotten_tomatoes_movies_path']} delimiter ',' CSV HEADER"
copy_staging_netflix = f"COPY staging_netflix_titles FROM {config['DATA_PATH']['netflix_titles_path']} delimiter ',' CSV HEADER"

copy_table_queries = [copy_staging_titles, copy_staging_ratings, copy_staging_principals, copy_staging_crew,
                      copy_staging_names, copy_staging_rt, copy_staging_netflix]

insert_genres = """\
INSERT INTO genres(title_id, genre_name)
   SELECT tconst, unnest(string_to_array(genres, ',')) as genre
   FROM staging_imdb_titles 
   WHERE genres!='\\N' 
   AND titleType in ('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')
   """

insert_persons = """\
INSERT INTO persons(id, full_name, birth_year, death_year)
SELECT nconst, primaryName, 
NULLIF(regexp_replace(birthYear, '\D','','g'), '')::numeric AS birth_year,
NULLIF(regexp_replace(deathYear, '\D','','g'), '')::numeric AS death_year
from staging_imdb_names"""

insert_roles = """\
INSERT INTO roles(title_id, person_id, role_name)
SELECT titles.tconst, nconst, category
FROM staging_imdb_titles titles
JOIN staging_imdb_principals principals
ON titles.tconst = principals.tconst
WHERE principals.category in ('director', 'writer', 'actor', 'actress') 
AND titles.titleType in ('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')

UNION
SELECT titles.tconst, unnest(string_to_array(directors, ',')) as nconst, 'director'
FROM staging_imdb_titles titles
JOIN staging_imdb_crew crew
ON titles.tconst = crew.tconst
WHERE directors!='\\N'
AND titles.titleType in ('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')

UNION
SELECT titles.tconst, unnest(string_to_array(writers, ',')) as nconst, 'writer'
FROM staging_imdb_titles titles
JOIN staging_imdb_crew crew
ON titles.tconst = crew.tconst
WHERE writers!='\\N'
AND titles.titleType in ('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')"""

insert_titles = """
INSERT INTO titles(netflix_id, imdb_id, rt_id, type, name, normalized_name,
  netflix_description, release_year, runtime_minutes, country, rated,
  imdb_avg_score, imdb_n_ratings,
  rt_critics_score, rt_n_critics,
  rt_audience_score, rt_n_audience, rt_critics_consensus)
  
SELECT staging_netflix_titles.show_id, with_imdb.tconst, with_rt.rotten_tomatoes_link,
staging_netflix_titles.type, staging_netflix_titles.title, lower(staging_netflix_titles.title),
staging_netflix_titles.description, staging_netflix_titles.release_year,
CAST(replace(with_imdb.runtimeMinutes, '\\N', '-1') as INT),
staging_netflix_titles.country,  staging_netflix_titles.rating,  
with_imdb.averageRating, with_imdb.numVotes, with_rt.tomatometer_rating, with_rt.tomatometer_count, with_rt.audience_rating, 
with_rt.audience_count, with_rt.critics_consensus

FROM staging_netflix_titles 
LEFT JOIN (
        SELECT distinct netflix.show_id, netflix.type, netflix.title, netflix.country, netflix.release_year, 
                netflix.description, imdb.tconst, runtimeMinutes, averageRating, numVotes
                FROM(
                    SELECT show_id, type, title, country, release_year, description,
                    unnest(string_to_array(staging_netflix_titles.director, ',')) as director,
                    unnest(string_to_array(staging_netflix_titles.cast, ',')) as actor
                    FROM staging_netflix_titles) as netflix
                JOIN(
                    SELECT *
                    FROM staging_imdb_titles 
                    JOIN roles
                    ON roles.title_id = staging_imdb_titles.tconst
                    JOIN persons 
                    ON persons.id = roles.person_id
                    WHERE titleType in ('tvSpecial', 'tvSeries', 'tvShort', 'movie', 'tvMovie', 'short', 'tvMiniSeries')) as imdb
                JOIN staging_imdb_ratings 
                    ON staging_imdb_ratings.tconst = imdb.tconst
            ON lower(netflix.title) = lower(imdb.OriginalTitle)
            AND netflix.release_year BETWEEN CAST(REPLACE(imdb.startYear,'\\N', '0') as INT) 
            AND CAST(REPLACE(imdb.endYear,'\\N', '2050') as INT) 
            AND (netflix.actor = imdb.full_name or netflix.director = imdb.full_name)
        ) as with_imdb
ON with_imdb.show_id = staging_netflix_titles.show_id

LEFT JOIN (
        SELECT distinct netflix.show_id, netflix.type, netflix.title, netflix.country, netflix.release_year, 
                netflix.description, rt.tomatometer_rating, rt.tomatometer_count, rt.audience_rating, rt.audience_count,
                rt.critics_consensus, rt.rotten_tomatoes_link
                FROM(
                    SELECT show_id, type, title, country, release_year, description,
                    unnest(string_to_array(staging_netflix_titles.director, ',')) as director,
                    unnest(string_to_array(staging_netflix_titles.cast, ',')) as actor
                    FROM staging_netflix_titles) as netflix
                JOIN(
                    SELECT rt.tomatometer_rating, rt.tomatometer_count, rt.audience_rating, rt.audience_count,
                    rt.critics_consensus, rt.movie_title, rt.original_release_date, rotten_tomatoes_link,
                    unnest(string_to_array(rt.directors, ',')) as director,
                    unnest(string_to_array(rt.actors, ',')) as actor
                    FROM staging_rotten_tomatoes_titles rt) rt
                ON lower(netflix.title) = lower(rt.movie_title)
                AND netflix.release_year = extract(year from rt.original_release_date)
                AND (netflix.director = rt.director or netflix.actor = rt.actor) 
                ) as with_rt
ON with_rt.show_id = staging_netflix_titles.show_id
"""
insert_table_queries = [insert_genres, insert_persons, insert_roles, insert_titles]
