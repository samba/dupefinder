#!/usr/bin/env python3

import sqlite3
import os
import sys
import hashlib
import logging
import contextlib
import argparse
import pathlib

from difflib import SequenceMatcher
from typing import List

DBNAME = "files.db"

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stderr))

def similarity(s1, s2):
    """Calculate the {0-1} similarity score of two series.
       Presently this is used for identifying directories as similar groups."""
    return SequenceMatcher(None, s1, s2).ratio()


def connect(dbname, **kwargs):
    conn = sqlite3.connect(dbname, **kwargs)
    conn.row_factory = sqlite3.Row
    conn.enable_load_extension(True)
    conn.enable_load_extension(False)
    conn.execute('PRAGMA foreign_keys = ON')
    return contextlib.closing(conn)


def init_database(con):
    yield """
        CREATE TABLE IF NOT EXISTS dirs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent INTEGER REFERENCES dirs(id) ON UPDATE CASCADE ON DELETE CASCADE,
            CONSTRAINT c_dirpath UNIQUE (name, parent)
        );
    """

    yield """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent INTEGER REFERENCES dirs(id) ON UPDATE CASCADE ON DELETE CASCADE,
            checksum TEXT NOT NULL,
            ts TIMETSTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT c_fileversion UNIQUE (name, parent, checksum)
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
                    -- TODO escape slashes in name
                    REPLACE((dirpath.path || "/" || B.name), '//', '/')
                FROM dirs B JOIN dirpath WHERE dirpath.id=B.parent
            )
            SELECT id, path FROM dirpath
        ;
    """

    yield "DROP VIEW IF EXISTS latest_files;"

    yield """
        CREATE VIEW IF NOT EXISTS latest_files AS
            SELECT
                f.name,
                f.parent,
                MAX(f.ts) as ts,
                b.id,
                b.checksum,
                p.path
            FROM files f
            LEFT OUTER JOIN files b USING (name, parent, ts)
            LEFT JOIN fulltree p ON p.id=f.parent
            GROUP BY f.name, f.parent
        ;
    """


    yield "DROP VIEW IF EXISTS dupe_files;"

    yield """
        CREATE VIEW IF NOT EXISTS dupe_files AS
            WITH dupe_csum (c, checksum) AS (
                    SELECT COUNT(name) as c, checksum
                    FROM latest_files
                    GROUP BY checksum
                    HAVING c > 1
                )
            SELECT
                a.id,
                a.parent,
                a.ts,
                a.checksum,
                a.name
            FROM dupe_csum d
                LEFT JOIN latest_files a ON d.checksum=a.checksum
            ORDER BY a.checksum
        ;
    """

    yield "DROP VIEW IF EXISTS parent_has_dupes;"

    yield """
        -- lists all paths that have any duplicates
        CREATE VIEW IF NOT EXISTS parent_has_dupes AS
            SELECT * FROM fulltree WHERE id IN (
                SELECT DISTINCT parent FROM dupe_files
            );
    """

    yield "DROP VIEW IF EXISTS parent_dir_match_score;"

    yield """
        CREATE VIEW IF NOT EXISTS parent_dir_match_score AS
        WITH
            matches AS (
                -- lists matching files, identified by name and checksum, and their parent paths (by id)
                SELECT
                    p1.id id1,
                    p2.id id2,
                    p1.parent p1,
                    p2.parent p2
                FROM dupe_files p1
                INNER JOIN dupe_files p2 ON (
                        p1.checksum=p2.checksum
                    AND p1.name=p2.name
                    AND p1.parent <> p2.parent
                    AND p1.parent < p2.parent -- avoids duplicates ({1,2} and {2,1})
                )
            ),
            contents_of_matches AS (
                SELECT DISTINCT
                    files.*
                FROM matches m
                INNER JOIN latest_files files ON (files.id=m.id2 OR files.id=m.id2)
                UNION
                SELECT DISTINCT
                    files.*
                FROM latest_files files
                WHERE files.id in (SELECT id FROM matching_dirs)
                ORDER BY files.checksum
            ),
            matching_dirs AS (
                SELECT
                    f1.path as path1,
                    f2.path as path2,
                    COUNT(1) as matchount,
                    (SELECT COUNT(1) FROM latest_files WHERE parent=m.p1) as p1total,
                    (SELECT COUNT(1) FROM latest_files WHERE parent=m.p2) as p2total
                FROM
                    matches m
                INNER JOIN fulltree f1 ON f1.id=m.p1
                INNER JOIN fulltree f2 ON f2.id=m.p2
                GROUP BY m.p1, m.p2
            )

            SELECT
                *,
                MAX((CAST(matchount as REAL) / p1total), (CAST(matchount as REAL) / p2total)) as mscore
            FROM matching_dirs
        ;
    """


def print_status(s):
    sys.stdout.write('\r')
    sys.stdout.write(s)
    sys.stdout.flush()


def escape(pathpart: str):
    return pathpart.replace('/', '\\/')


def _hash(filename, factory=hashlib.md5, block_count=128):
    csum = factory()
    readsize = block_count * (csum.block_size or 8192)
    with open(filename, 'rb') as f:
        for chunk in iter(lambda: f.read(readsize), b''):
            csum.update(chunk)
    return csum



def exclusions(patterns):
    """
    >>> p = pathlib.PurePath
    >>> x = exclusions(['**/a.py', '*/x*x.py', '**/x.py', '**/popcorn/**',
    ...                 '**/pineapple'])
    >>> list(filter(x, [p('/a.py'), p('b/x.py'), p('b/x2x.py')]))
    []
    >>> list(filter(x, [p('/a/pizza/popcorn/pineapple/a.py'), p('/a/none/y.py')]))
    [PurePosixPath('/a/none/y.py')]
    >>> list(filter(x, [p('/a/popcorn/z.py'), p('/a/popcorn'), p('/a/pineapple')]))
    [PurePosixPath('/a/popcorn')]
    """
    return lambda p: not any(p.match(x) for x in patterns)


def scandir(exclude, *dirpaths):
    ex = exclusions(exclude)
    yield True, "", None, None  # seed the root directory in the database
    for d in dirpaths:
        d = os.path.abspath(d)  # absolute path only
        logger.info('Scanning directory: %s', d)

        yield True, d, "", None     # top level directories refer to root

        for root, dirs, files in os.walk(d, topdown=True):

            p = pathlib.PurePath(root)

            # Produce PurePath instances that are not excluded
            _files = list(filter(ex, [(p / f) for f in files]))
            _dirs = list(filter(ex, [(p / d) for d in dirs]))

            if (len(_files) == len(_dirs)) and len(_dirs) == 0:
                dirs[:] = []  # terminate the iteration below this point

            for fpath in _files:
                logger.info('Scanning file: %s' % (fpath))
                try:
                    csum = _hash(str(fpath))
                except (FileNotFoundError, PermissionError, IOError) as e:
                    logger.error('Failed to read: %s', str(fpath))
                    logger.error(e)
                    continue
                yield False, fpath.name, str(fpath.parent), csum.hexdigest()

            for dpath in _dirs:
                yield True, dpath.name, str(dpath.parent), None


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
                logger.debug(f'''
                    Inserting {"dir" if is_dir else "file"}: {fpath!r} in {parent!r}
                    '''.strip())
                if is_dir:
                    cur.execute(
                        'INSERT OR IGNORE INTO dirs (name, parent) ' +
                        'VALUES (?, (SELECT id FROM fulltree WHERE path=? LIMIT 1))',
                        (fpath, parent))
                    cur.commit()  # necessary for foreign key resolution
                else:
                    cur.execute(
                        'INSERT OR IGNORE INTO files (name, parent, checksum) ' +
                        'VALUES (?, (SELECT id FROM fulltree WHERE path=? LIMIT 1), ?)',
                        (fpath, parent, csum))

            cur.commit()  # save inserts
            print_status('\n')


def print_row(*rows: sqlite3.Row):
    for r in rows:
        print(dict(r))


def finddupefiles(parentpath=None):
    """ Searches for duplicate files, limiting the results to those in a given path."""

    with connect(DBNAME) as conn:
        with conn as cur:
            if parentpath:
                parentpath = os.path.abspath(parentpath) + os.sep
            logger.info('Scanning duplicates in prefix: %s', parentpath)
            query = f"""
                WITH dupe_csum (c, checksum) AS (
                    SELECT COUNT(id) as c, checksum
                    FROM latest_files
                    -- NB: this can limit the scope of dupe evaluation within a path
                    -- {f'WHERE path LIKE "{parentpath}%"' if parentpath else ''}
                    GROUP BY checksum
                    HAVING c > 1
                )
                SELECT
                    a.id,
                    (b.path || '/' || a.name) as path,
                    a.ts,
                    a.checksum
                FROM dupe_csum d
                    LEFT JOIN latest_files a ON d.checksum=a.checksum
                    LEFT JOIN fulltree b ON b.id=a.parent
                {f'WHERE a.path LIKE "{parentpath}%"' if parentpath else ''}
                ORDER BY a.checksum, b.path
            """
            # logger.debug(query)
            result = cur.execute(query)
            yield from result.fetchall()


def finddupedirs(parentpath=None, similarity=0.5):
    """ Searches for directories whose contents are identical,
        or supersets of another."""
    with connect(DBNAME) as conn:
        with conn as cur:
            if parentpath:
                parentpath = os.path.abspath(parentpath) + os.sep
        query = """
            SELECT * FROM parent_dir_match_score
            WHERE mscore > ?
        """
        result = cur.execute(query, (similarity,))
        yield from result.fetchall()

def print_dupe_file(row: sqlite3.Row):
    print(f"{row['checksum']:.32s}\t{row['path']}")


def print_dupe_dirs(row: sqlite3.Row):
    print(f"{row['path1']}\t{row['path2']}")


def options(*args):
    parser = argparse.ArgumentParser(
        description=("Scans one or more directories to identify duplicates and"
                     + " similar directories.")
    )
    parser.add_argument('-x', '--exclude', nargs=1, help='exclude patterns')
    parser.add_argument('dirs', nargs='*', help='directories to scan')
    return parser.parse_args(args)



def main(verb, *args):
    opts = options(*args)

    if verb == 'scan':
        loaddirs(scandir(opts.exclude or [], *opts.dirs))
    elif verb == 'dupes':
        ex = exclusions(opts.exclude or [])
        for p in (opts.dirs or [None]):
            for r in finddupefiles(parentpath=p):  # type: sqlite3.Row
                if not ex(pathlib.PurePath(r['path'])):
                    print_dupe_file(r)
    elif verb == 'dupedirs':
        ex = exclusions(opts.exclude or [])
        for p in (opts.dirs or [None]):
            for r in finddupedirs(parentpath=p):  # type: sqlite3.Row
                if not (ex(pathlib.PurePath(r['path1'])) and ex(pathlib.PurePath(r['path2']))):
                    print_dupe_dirs(r)


if __name__ == '__main__':
    main(*sys.argv[1:])
