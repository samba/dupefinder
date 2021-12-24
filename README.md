# Dupe Finder

**This project is still an early prototype.**

## What problem does this solve?

I have accumulated a pile of backup archives over my decades working in IT & tech.
I need to clean these up for permanent cold-strage archival, remove duplicate and partial copies.
I'm a busy person and don't have time to manually inspect all of it, so I need a program to help me find the various replicas.

## How does this work?

Scans one or more directories, collecting checksums on all files found.
Stores found checksums, with timestamps, in a SQLite database.
Queries database to find:

* fully duplicated files.
* directories with >50% "similar" files, having the same name and same checksum.

This tool is intended to be used to _inform_ cleanup/archivale processes. It does not delete anything directly.

## Future capabilities

* classifying scanned directories based on inferred _completeness_ by relative number of files
* generating shell scripts to remove files and directories based on priority rules



