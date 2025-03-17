_default:
    @ just --list -u

# TODO: run ansible ci
[doc("mimics the CI pipeline")]
run_ci: && test_all
    cargo check
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

[doc("Runs all Rust unit tests")]
test_unit:
    cargo build --workspace --tests
    cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

[doc("Runs all tests")]
test_all: _setup && _go_test
    cargo test --workspace --no-fail-fast -- --nocapture --test-threads=1

[doc("Runs rust end-to-end tests")]
test_end_to_end: _setup
    cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

[doc("Runs Go migration tests")]
test_migrations: _setup && _go_test

[doc("Runs the kafka integration test")]
test_kafka: _setup
    cargo test --test end_to_end test_kafka --features debug --no-fail-fast -- --nocapture --test-threads=1

# Without `-count=1` tests are cached
[working-directory: 'migrations'] # requires just 1.39.0
_go_test:
    go test -v -count 1 ./...

[doc("Runs the specified Rust test")]
test TEST: _setup
    cargo test {{TEST}} --features debug --no-fail-fast -- --nocapture --test-threads=1

[doc("psql into the container database")]
psql:
    @ docker exec -it lard_tests psql -U postgres

_setup: _clean_if_running
    @ echo "Starting Postgres docker container..."
    docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
    @ echo; sleep 3
    cargo build --workspace --tests
    @ echo; echo "Loading DB schema..."; echo
    @target/debug/prepare_postgres

_clean_if_running:
    @ if docker ps -a | grep lard_tests > /dev/null; then just _clean > /dev/null; fi

_clean:
    @ echo "Stopping Postgres container..."
    @ docker stop lard_tests
    @ echo "Removing Postgres container..."
    @ docker rm lard_tests
