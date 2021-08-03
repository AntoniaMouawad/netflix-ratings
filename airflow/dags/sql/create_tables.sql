-- Create staging tables
CREATE TABLE IF NOT EXISTS staging_imdb_titles(
tconst VARCHAR NOT NULL PRIMARY KEY,
titleType VARCHAR,
primaryTitle VARCHAR,
originalTitle VARCHAR,
isAdult VARCHAR,
startYear VARCHAR,
endYear VARCHAR,
runtimeMinutes VARCHAR,
genres VARCHAR);

CREATE TABLE IF NOT EXISTS staging_imdb_ratings(
tconst VARCHAR NOT NULL,
averageRating FLOAT,
numVotes INT);

CREATE TABLE IF NOT EXISTS staging_imdb_principals(
tconst VARCHAR NOT NULL,
ordering INTEGER NOT NULL,
nconst VARCHAR NOT NULL,
category VARCHAR NOT NULL,
job VARCHAR NOT NULL,
characters VARCHAR NOT NULL);

CREATE TABLE IF NOT EXISTS staging_imdb_crew(
tconst VARCHAR NOT NULL,
directors VARCHAR,
writers VARCHAR);

CREATE TABLE IF NOT EXISTS staging_imdb_names(
nconst VARCHAR NOT NULL PRIMARY KEY,
primaryName VARCHAR NOT NULL,
birthYear VARCHAR,
deathYear VARCHAR,
primaryProfession VARCHAR,
knownForTitles VARCHAR);

CREATE TABLE IF NOT EXISTS staging_rotten_tomatoes_titles(
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

CREATE TABLE IF NOT EXISTS staging_netflix_titles(
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

-- Create final tables
CREATE TABLE IF NOT EXISTS titles(
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

CREATE TABLE IF NOT EXISTS roles(
id SERIAL PRIMARY KEY,
title_id VARCHAR,
person_id VARCHAR,
role_name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS persons(
id VARCHAR NOT NULL PRIMARY KEY,
full_name VARCHAR NOT NULL,
birth_year INT4,
death_year INT4
);

CREATE TABLE IF NOT EXISTS genres(
id SERIAL PRIMARY KEY,
title_id VARCHAR NOT NULL,
genre_name VARCHAR NOT NULL);
