_default:
    @ just --list -u

[doc("mimics the CI pipeline")]
run_ci: && test_all
    cargo check
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings

# TODO: run_ansible_ci:

test_unit:
    cargo build --workspace --tests
    cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

test_all: setup && _go_test
    cargo test --workspace --no-fail-fast -- --nocapture --test-threads=1

test_end_to_end: setup
    cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

test_migrations: setup && _go_test

test_kafka: setup
    cargo test --test end_to_end test_kafka --features debug --no-fail-fast -- --nocapture --test-threads=1

# Without `-count=1` tests are cached
[working-directory: 'migrations'] # requires just 1.39.0
_go_test:
    go test -v -count 1 ./...

test TEST: setup
    cargo test {{TEST}} --features debug --no-fail-fast -- --nocapture --test-threads=1

[doc("psql into the container database")]
psql:
    @ docker exec -it lard_tests psql -U postgres

_clean_if_running:
    @ if docker ps | grep lard_tests > /dev/null; then just clean > /dev/null; fi

setup: _clean_if_running
    @ echo "Starting Postgres docker container..."
    docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
    @ echo; sleep 3
    cargo build --workspace --tests
    @ echo; echo "Loading DB schema..."; echo
    @target/debug/prepare_postgres

clean:
    @ echo "Stopping Postgres container..."
    @ docker stop lard_tests
    @ echo "Removing Postgres container..."
    @ docker rm lard_tests
