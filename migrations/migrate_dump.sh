#!/bin/bash

go build

# dump data from kvalobs in 2 year windows
for from_year in {2008..2024..2}; do
    to_year=$((from_year + 2))

    echo "Dumping kvalobs data from $from_year to $to_year"

    tmux new-session\; send "./migrate kvalobs dump \
      --path /mnt/dumps \
      --from $from_year-01-01 \
      --to $to_year-01-01" ENTER \; detach

    echo
done

echo "Dumping tables from KDVH"
tmux new-session\; send "./migrate kdvh dump" ENTER \; detach
