#!/bin/bash
for from_year in {2008..2024..2}; do
  echo "Current from: $from_year"
  to_year=$((from_year+2))
  echo "Current to: $to_year"
  echo
  tmux new-session\; send "./migrate kvalobs dump --path /mnt/dumps --from $from_year-01-01 --to $to_year-01-01" ENTER \; detach
done
