# Migrations

Go package that dumps tables from old databases (KDVH, Kvalobs) and imports them into LARD.

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

1. Before you dump or import you need to make sure you have a `.env` file with all the environment variables needed.
   These are

   1. LARD_CONN_STRING
   1. LARD_RESTRICTED_CONN_STRING
   1. STINFO_CONN_STRING
   1. KDVH_PROXY_CONN_STRING
   1. KVALOBS_CONN_STRING
   1. HISTKVALOBS_CONN_STRING

### Dump

1. In order to dump, you can either use the dump script

   ```terminal
   # Dumps all data from both kvalobs and KDVH into the /mnt/dumps directory in separate tmux sessions 
   # IMPORTANT: make sure there's enough space on disk
   bash migrate_dump.sh
   ```

   or call the following commands separately with the options that you need

   ```terminal
   ./migrate kdvh dump <options>
   ./migrate kvalobs dump <options>
   ```

   You can use the `--help` flag to see all available options.
   Since these are long running tasks, it's recommended to run these inside `tmux` sessions.

### Import

1. From your local environment, setup the database for import

   ```terminal
   cd ../ansible
   uv run ansible-playbook -i staging.yml migration_setup.yml -t pre
   ```

1. Connect to the migration VM and start a tmux session

   ```terminal
   cd lard/migrations
   tmux
   ```

1. In order to import dumps into LARD, you can use the import script

   ```terminal
   # Imports all the data present in <dump_dir> (defaults to /mnt/dumps if you used the dump script)
   GOMEMLIMIT=6GiB bash migrate_import.sh <dump_dir>
   ```

   This script drops all indices and constraints, so that the COPY FROM runs as fast as possible.
   These indices and constraints are then rebuilt after all the data has been imported.

   Dropping and recreating indices is only necessary when importing big amounts of data.
   If you want to only import some timeseries you can call the following commands separately with the options that you need

   ```terminal
   ./migrate kdvh import <options>
   ./migrate kvalobs import <options>
   ```

   You can use the `--help` flag to see all available options.
   Since these are long running tasks, it's recommended to run these inside `tmux` sessions.

1. After the import is complete, you can run the post migration setup from your local environment

   ```terminal
   cd ../ansible
   uv run ansible-playbook -i staging.yml migration_setup.yml -t post
   ```

## Other notes

Insightful talk on migrations: [here](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s)
