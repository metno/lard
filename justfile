_default:
    @ just --list -u

# TODO: run ansible ci
[doc("Mimic the CI pipeline")]
run_ci: && test_all
    cargo check
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

[doc("Run all Rust unit tests")]
test_unit:
    cargo build --workspace --tests
    cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

[doc("Run all tests")]
test_all: _setup && _go_test
    cargo test --workspace --no-fail-fast -- --nocapture --test-threads=1

[doc("Run rust end-to-end tests")]
test_e2e: _setup
    cargo test -p lard_tests --no-fail-fast -- --nocapture --test-threads=1

[doc("Run only end-to-end tests in the specified test target")]
test_e2e_only test: _setup
    cargo test -p lard_tests --test {{test}} --no-fail-fast -- --nocapture --test-threads=1

[doc("Run Go migration tests")]
test_migrations: _setup && _go_test

# Without `-count=1` tests are cached
[working-directory: 'migrations'] # requires just 1.39.0
_go_test:
    go test -v -count 1 ./...

[doc("Run the specified Rust e2e test")]
test name: _setup
    cargo test {{name}} -p lard_tests --features debug --no-fail-fast -- --nocapture --test-threads=1

[doc("psql into the container database")]
psql db="lard":
    @ docker exec -it lard_tests psql -U postgres -d {{db}}

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
