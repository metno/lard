# Migrations

Go package that dumps tables from legacy databases (KDVH, Kvalobs) and imports them into LARD.

## Usage

### General

1. Connect to the migration VM

1. If the LARD repo is not already present, git clone it (using HTTPs)

1. If working on a feature branch, do the following (needed due to HTTPs)

   ```terminal
   git switch -c <branch_name>
   git pull origin <branch_name>
   ```

1. Compile the package with

   ```terminal
   go build 
   # outputs a 'migrate' executable
   ```

1. Before you dump or import, make sure you have an `.env` file with all the environment variables needed.
   These are:

   - LARD_CONN_STRING
   - LARD_RESTRICTED_CONN_STRING
   - STINFO_CONN_STRING
   - KDVH_PROXY_CONN_STRING
   - KVALOBS_CONN_STRING
   - HISTKVALOBS_CONN_STRING

### Dump

> [!IMPORTANT]
> Since the following are long running tasks, it's recommended to run them inside `tmux` sessions.
> Also make sure there is enough disk space.

> [!TIP]
> All of the `migrate` commands accept a `--help` flag explaining all the options of the given command.
> In particular it's possible to filter specific stations, parameters, etc. and/or to dump data only in a specific interval.

1. Dump data from KDVH

   ```terminal
   tmux
   ./migrate kdvh dump -p /mnt/dumps/kdvh
   ```

1. Dump from histkvalobs

   A script is provided to dump all data in 2 year partitions

   ```terminal
   bash dump_histkvalobs.sh
   ```

1. Dump from kvalobs

   In theory Kvalobs keeps only the last ~3 months of data, however the migrations from
   kvalobs to histkvalobs are performed manually, so there might be old data
   that has not been deleted. This can cause problems during import, so we need
   to dump only the data that we are certain is not yet in histkvalobs.

   1. Check the last timestamp of a timeseries in the last partition from the previous step, for example

      ```terminal
      tail -n 1 /mnt/dumps/histkvalobs/from_2024-01-01_to_2026-01-01/data/18700/18700_211_501_0_0.csv
      # Gives the timestamp: 2025-07-31T23:00:00Z
      ```

   1. Use the the day after as a starting point for the kvalobs dumps

      ```terminal
      tmux
      ./migrate kvalobs dump -p /mnt/dumps --db kvalobs --from 2025-08-01
      ```

### Import

1. From your local environment, setup the database for import

   ```terminal
   cd ../ansible
   uv run ansible-playbook -i staging.yml playbooks/migration_setup.yml -t pre,truncate
   ```

1. Connect to the migration VM and start a tmux session

   ```terminal
   cd lard/migrations
   tmux
   ```

1. In order to import dumps into LARD, you can use the import script

   > [!NOTE]
   > GOMEMLIMIT needs to be set in order to avoid potential OOM issues

   ```terminal
   # Imports all the data present in /mnt/dumps
   GOMEMLIMIT=6GiB bash migrate_import.sh /mnt/dumps
   ```

   This script drops all indices and constraints, so that the COPY FROM runs as fast as possible.
   These indices and constraints are then rebuilt after all the data has been imported.

   Dropping and recreating indices is only necessary when importing big amounts of data.
   If you want to only import some timeseries you can call the following commands separately with the options that you need

   ```terminal
   GOMEMLIMIT=6GiB ./migrate kdvh import <options>
   GOMEMLIMIT=6GiB ./migrate kvalobs import <options>
   ```

   You can use the `--help` flag to see all available options.

1. After the import is complete, you can run the post migration setup from your local environment

   ```terminal
   cd ../ansible
   uv run ansible-playbook -i staging.yml playbooks/migration_setup.yml -t post
   ```

## Other notes

Insightful talk on migrations: [here](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s)
