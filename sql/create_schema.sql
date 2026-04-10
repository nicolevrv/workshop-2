-- create_schema.sql
-- Creates the music_dw database and all star schema tables.
-- Safe to re-run: DROP IF EXISTS ensures a clean slate each execution.

CREATE DATABASE IF NOT EXISTS music_dw
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE music_dw;

-- ── Drop in reverse FK order ──────────────────────────────────────────────────
DROP TABLE IF EXISTS fact_track;
DROP TABLE IF EXISTS dim_album;
DROP TABLE IF EXISTS dim_genre;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS dim_artist;

-- ── dim_artist ────────────────────────────────────────────────────────────────
CREATE TABLE dim_artist (
    artist_key          INT             NOT NULL AUTO_INCREMENT,
    artist_norm         VARCHAR(255)    NOT NULL,
    artist_name         VARCHAR(255),
    grammy_nominations  INT             NOT NULL DEFAULT 0,
    grammy_wins         INT             NOT NULL DEFAULT 0,
    first_grammy_year   SMALLINT,
    last_grammy_year    SMALLINT,
    PRIMARY KEY (artist_key),
    UNIQUE KEY uq_artist_norm (artist_norm)
);

-- ── dim_album ─────────────────────────────────────────────────────────────────
CREATE TABLE dim_album (
    album_key   INT             NOT NULL AUTO_INCREMENT,
    album_name  VARCHAR(500)    NOT NULL,
    artist_norm VARCHAR(255)    NOT NULL,
    PRIMARY KEY (album_key),
    KEY idx_album_artist (artist_norm)
);

-- ── dim_genre ─────────────────────────────────────────────────────────────────
CREATE TABLE dim_genre (
    genre_key   INT             NOT NULL AUTO_INCREMENT,
    genre_name  VARCHAR(100)    NOT NULL,
    PRIMARY KEY (genre_key),
    UNIQUE KEY uq_genre_name (genre_name)
);

-- ── dim_time ──────────────────────────────────────────────────────────────────
CREATE TABLE dim_time (
    time_key    INT         NOT NULL AUTO_INCREMENT,
    year        SMALLINT    NOT NULL,
    decade      SMALLINT    GENERATED ALWAYS AS (FLOOR(year / 10) * 10) STORED,
    PRIMARY KEY (time_key),
    UNIQUE KEY uq_year (year)
);

-- ── fact_track ────────────────────────────────────────────────────────────────
CREATE TABLE fact_track (
    track_id        VARCHAR(100)    NOT NULL,
    track_name      VARCHAR(500),
    artist_key      INT,
    album_key       INT,
    genre_key       INT,
    time_key        INT,
    popularity          TINYINT UNSIGNED,
    duration_ms         INT,
    danceability        FLOAT,
    energy              FLOAT,
    valence             FLOAT,
    tempo               FLOAT,
    loudness            FLOAT,
    acousticness        FLOAT,
    instrumentalness    FLOAT,
    liveness            FLOAT,
    speechiness         FLOAT,
    explicit            TINYINT(1)  DEFAULT 0,
    grammy_nominations  INT         NOT NULL DEFAULT 0,
    grammy_wins         INT         NOT NULL DEFAULT 0,
    PRIMARY KEY (track_id),
    FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key)
        ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (album_key)  REFERENCES dim_album(album_key)
        ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (genre_key)  REFERENCES dim_genre(genre_key)
        ON DELETE SET NULL ON UPDATE CASCADE,
    FOREIGN KEY (time_key)   REFERENCES dim_time(time_key)
        ON DELETE SET NULL ON UPDATE CASCADE,
    KEY idx_fact_artist  (artist_key),
    KEY idx_fact_genre   (genre_key),
    KEY idx_fact_time    (time_key),
    KEY idx_fact_popular (popularity)
);