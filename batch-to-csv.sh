
cat ds/dumped-ratings.json\
  | jq -r '(.results[0]) | .columns,(.data[].row | select(.[1] | contains(",") != true)) | @csv' > ds/dumped-ratings.csv
