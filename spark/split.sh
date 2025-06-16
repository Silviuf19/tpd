#!/bin/bash

# jq -c '.[]' jsonOutput3.json > flat.json
# split -l 10000 flat.json chunk_


for f in chunk_*; do
  echo "[" > "$f.json"
  sed '$!s/$/,/' "$f" >> "$f.json"
  echo "]" >> "$f.json"
done
