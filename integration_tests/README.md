# Tests

## End-to-end

End-to-end tests are implemented inside `integration_tests\tests\end_to_end.rs`.

1. Each test is defined inside a wrapper function (`e2e_test_wrapper`) that
   spawns separate tasks for the ingestor, the API server, and a Postgres
   client that cleans up the database after the test is completed.

1. Each test defines a `TestData` struct (or an array of structs) that contains
   the metadata required to build a timeseries. The data is formatted into an
   Obsinn message, which is sent via a POST request to the ingestor.

1. Finally, the data is retrived and checked by sending a GET request to one of
   the API endpoints.

> \[!IMPORTANT\]
> When implementing new tests remember to use one of the **open** station IDs
> defined in the `mock_permit_tables` function, otherwise the ingestor will not be able to
> insert the data into the database.

If you have Docker installed, you can run the tests locally using the provided
`Makefile`:

```terminal
# Run all tests
make test_all

# Run unit tests only
make unit_tests

# Run integration tests only
make end_to_end

# Debug a specific test (does not clean up the DB if `my_test_name` is an integration test)
TEST=my_test_name make debug_test

# If any error occurs while running integration tests, you might need to reset the DB container manually
make clean
```
