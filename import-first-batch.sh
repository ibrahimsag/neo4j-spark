#!/bin/sh

sudo -u neo4j rm -rf /var/lib/neo4j/data/books.db/
sudo \
  -u neo4j \
  neo4j-import \
    --into /var/lib/neo4j/data/books.db \
    --delimiter="|" \
    --nodes ds/books.csv \
    --nodes ds/users.csv \
    --relationships ds/ratings.csv
