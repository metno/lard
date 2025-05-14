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

venv := ".lard_tests_venv"
bin := venv/"bin"
s3_bucket := "latest"
_setup: _clean
    docker compose -f compose.yml up -d
    @ echo "Setting up S3 bucket..."
    @ python3 -m venv {{venv}}
    @ {{bin}}/python3 -m pip install awscli-local[ver1] > /dev/null
    @ {{bin}}/awslocal s3 mb s3://{{s3_bucket}} > /dev/null
    @ echo "Waiting for DB readiness..."; sleep 3
    cargo build --bins
    @ echo "Setting up test environment..."
    @ target/debug/prepare_postgres

_clean:
    docker compose down
