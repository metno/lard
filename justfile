set dotenv-filename := "integration_tests/.env.test"
set dotenv-override := true

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
test_e2e_only target: _setup
    cargo test -p lard_tests --test {{target}} --no-fail-fast -- --nocapture --test-threads=1

[doc("Run Go migration tests")]
test_migrations: _setup && _go_test

# Without `-count=1` tests are cached
[working-directory: 'migrations'] # requires just 1.39.0
_go_test:
    go test -v -count 1 ./...

[doc("Run the specified Rust e2e test")]
test name: _setup
    cargo test {{name}} -p lard_tests --features debug --no-fail-fast -- --nocapture --test-threads=1 --exact

[doc("psql into the container database")]
psql db="lard":
    @ docker exec -it lard_postgres psql -U postgres -d {{db}}

# TODO: We are creating a bucket with awslocal because there is currently a bug
# in `rust-s3` that prevents bucket creation in local environments, see
# https://github.com/durch/rust-s3/issues/411
# Eventually we want to create the bucket directly in rust when that bug is resolved.
_setup: _clean
    docker compose -f $COMPOSE_YAML up -d
    @ echo "Waiting for DB readiness..."; sleep 3
    cargo build --bins
    @ echo "Setting up test environment..."
    @ target/debug/setup_test_environment

_clean:
    docker compose -f $COMPOSE_YAML down

_setup_frost_e2e: _clean_frost_e2e
    docker compose -f $FROST_COMPOSE_YAML up -d

_clean_frost_e2e:
    docker compose -f $FROST_COMPOSE_YAML down
