#!/usr/bin/env python3

import sqlite3
import os
import sys
import hashlib
import logging
import contextlib

from difflib import SequenceMatcher
from typing import List

DBNAME = "files.db"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stderr))

def similarity(s1, s2):
    return SequenceMatcher(None, s1, s2).ratio()


def connect(dbname, **kwargs):
    conn = sqlite3.connect(dbname, **kwargs)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    conn.enable_load_extension(False)
    return contextlib.closing(conn)


def scandir(*dirpaths):
    for d in dirpaths:
        d = os.path.abspath(d)  # absolute path only
        logger.debug('Scanning directory: %s', d)
        yield True, d, os.path.dirname(d), None
        for root, dirs, files in os.walk(d, topdown=True):
            for name in files:
                fpath = os.path.join(root, name)
                csum = hashlib.md5(open(fpath, 'rb').read())
                yield False, name, root, csum.hexdigest()
            for name in dirs:
                fpath = os.path.join(root, name)
                yield True, name, root, None


def init_database(con):
    yield """
        CREATE TABLE IF NOT EXISTS dirs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            parent INTEGER
        );
    """

    yield """
        CREATE TABLE IF NOT EXISTS files (
            name TEXT,
            parent INTEGER,
            checksum TEXT
        );
    """

    yield "DROP VIEW IF EXISTS fulltree;"

    yield """
        CREATE VIEW IF NOT EXISTS fulltree AS
            WITH RECURSIVE
            dirpath (id, name, path) AS (
                SELECT
                    A.id,
                    A.name,
                    A.name
                FROM dirs A WHERE A.parent is NULL
                UNION ALL
                SELECT
                    B.id,
                    B.name,
                    (dirpath.path || "/" || B.name)
                FROM dirs B JOIN dirpath WHERE dirpath.id=B.parent
            )
            SELECT id, path FROM dirpath
        ;
    """


def loaddirs(paths):
    with connect(DBNAME) as conn:
        with conn as cur:
            for q in init_database(cur):
                try:
                    cur.execute(q)
                except Exception as e:
                    logger.debug(e)
                    logger.debug(q)
            
            cur.commit()  # apply structure

            for is_dir, fpath, parent, csum in paths:
                if is_dir:
                    cur.execute(
                        'INSERT INTO dirs (name,parent) ' +
                        'VALUES (?, (SELECT id FROM fulltree WHERE path=?))',
                        (fpath, parent))
                else:
                    cur.execute(
                        'INSERT INTO files (name, parent, checksum) ' +
                        'VALUES (?, (SELECT id FROM fulltree WHERE path=?), ?)',
                        (fpath, parent, csum))
            
            cur.commit()  # save inserts


def print_row(*rows: sqlite3.Row):
    for r in rows:
        print(dict(r))


def finddupefiles(parentpath=None):
    """ Searches for duplicate files."""

    with connect(DBNAME) as conn:
        with conn as cur:
            result = cur.execute("""
                WITH dupe_csum (c, checksum) AS (
                        SELECT COUNT(name) as c, checksum
                        FROM files
                        GROUP BY checksum
                    )
                SELECT * FROM files
                WHERE checksum IN (SELECT checksum FROM dupe_csum WHERE c > 1);
            """)
            yield from result.fetchall()


def finddupedirs(parentpath=None):
    """ Searches for directories whose contents are identical,
        or supersets of another."""
    pass


def main(verb, *args):

    if verb == 'scan':
        loaddirs(scandir(*args))
    elif verb == 'dupes':
        for r in finddupefiles(*args):  # type: sqlite3.Row
            print_row(r)


if __name__ == '__main__':
    main(*sys.argv[1:])
