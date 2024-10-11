# KDVH Importer

Go package used to dump tables from the KDVH database and then import them into LARD.

## Usage

1. Compile it with

   ```terminal
   go build
   ```

1. Dump tables from KDVH

   ```terminal
   ./kdvh-importer dump --help
   ```

1. Import dumps into LARD

   ```terminal
   ./kdvh-importer import --help
   ```

## Useful design notes

Taken from this [talk](https://www.youtube.com/watch?v=wqXqJfQMrqI&t=280s):

1. 7 Rs: Relocate, Rehost, Replatform, Refactor, Rearchitect, Rebuild, Repurchase

1. Write migration as code

1. Model data from the migrating systems as different types and write code that converts between them

1. Split migration in discreet steps (isolated?)

1. Have explicit asserts

1. Log to file

1. Comment, especially regarding edge cases

1. Use progress bars

1. Perform test migrations (against "real" service if possible, i.e. in Ostack?)

1. Store IDs from old systems in new system

1. Migration is not a clean process, it's okay to write custom code for edge
   cases. But treat them as a separate thing, don't try to make them fit with
   the rest.

1. There might be things that we don't need to migrate. Document why!
