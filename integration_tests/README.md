# Tests

## End-to-end

These are implemented inside `integration_tests\tests\end_to_end.rs`.

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
# Run all tests


# Run only unit tests


# Run only integration tests
make end_to_end

# Debug a specific test (does not clean up the DB)
TEST=my_test_name make debug_test

# If any error occurs in the integration tests, you might need to reset the DB container manually
make clean
```
