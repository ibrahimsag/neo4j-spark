curl -H accept:application/json -H content-type:application/json \
     -d '{"statements":[{"statement":
        "MATCH (u:User)-[r:Rated]-(b:Book) RETURN u.id as userId, b.id as isbn, r.rating as rating"
      }]}' \
     http://localhost:7474/db/data/transaction/commit\
     | jq -r '(.results[0]) | .columns,.data[].row | @csv' > ds/dumped-ratings.csv
