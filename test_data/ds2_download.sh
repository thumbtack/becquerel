#!/bin/sh

# Download the Dell DVD Store test data set into the ds2 folder in the current directory.

# Give up if anything bad happens.
set -o errexit
set -o nounset

# Download and unpack files from Dell.
BASE_URL='https://linux.dell.com/dvdstore/2011-12-01'
curl -s "${BASE_URL}/ds21.tar.gz" | tar -xz
curl -s "${BASE_URL}/ds21_mysql.tar.gz" | tar -xz
curl -s "${BASE_URL}/ds21_postgresql.tar.gz" | tar -xz

# The category list is stored in DB-specific formats,
# so we generate a CSV here for other DBs.
cat > ds2/data_files/categories.csv <<- 'CSV'
1,Action
2,Animation
3,Children
4,Classics
5,Comedy
6,Documentary
7,Drama
8,Family
9,Foreign
10,Games
11,Horror
12,Music
13,New
14,Sci-Fi
15,Sports
16,Travel
CSV
