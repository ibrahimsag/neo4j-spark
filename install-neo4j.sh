#!/bin/sh
wget -O - http://debian.neo4j.org/neotechnology.gpg.key | apt-key add -
echo 'deb http://debian.neo4j.org/repo stable/' > /etc/apt/sources.list.d/neo4j.list
apt-get update
apt-get install neo4j
service neo4j-service status

# tell neo4j to use the books.db for database location
# just place/update this line in the target file.
echo "org.neo4j.server.database.location=data/books.db" >> /etc/neo4j/neo4j-server.properties
echo "org.neo4j.server.webserver.address=0.0.0.0" >> /etc/neo4j/neo4j-server.properties
echo "dbms.security.auth_enabled=false" >> /etc/neo4j/neo4j-server.properties
service neo4j-service restart
