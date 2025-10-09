# Migrations

Go package that dumps tables from old databases (KDVH, Kvalobs) and imports them into LARD.

## Usage

1. Compile it with

   ```terminal
   go build
   ```

1. Dump tables

   ```terminal
   ./migrate kdvh dump
   ./migrate kvalobs dump
   ```

1. Import dumps into LARD

   ```terminal
   ./migrate kdvh import
   ./migrate kvalobs import
   ```

For each command, you can use the `--help` flag to see all available options.

## Migration guide

1. Dump from legacy databases
1. Setup the database for import
   ```terminal
   ansible-playbook -i inventory.yml migration_setup.yml -t pre
   ```
1. Import dumps
1. Run the post migration setup
   ```terminal
   ansible-playbook -i inventory.yml migration_setup.yml -t post
   ```

## Other notes

Insightful talk on migrations: [here](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s)
