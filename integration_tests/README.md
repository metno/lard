# Integration tests

## End-to-end

1. Each test is defined inside a wrapper function (`e2e_test_wrapper`) that
   spawns separate tasks for the ingestor, the API server, and a Postgres
   client that cleans up the database after the test is completed.

1. Each test defines a `TestData` struct (or an array of structs) that contains
   the metadata required to build a timeseries. The data is formatted into an
   Obsinn message, which is sent via a POST request to the ingestor.

1. Finally, the data is retrived and checked by sending a GET request to one of
   the API endpoints.

If you have Docker installed, you can run the tests locally using the provided
`Makefile`:

```terminal
# Run all integration tests
make end_to_end

# Debug a specific test (does not clean up the DB)
TEST=my_test_name make debug_test

# If any error occurs, you might need to reset the DB container manually
make clean
```
