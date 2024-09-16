# Basically the same as definying all targets .PHONY
MAKEFLAGS += --always-make

unit_tests:
	@cargo build --workspace --tests
	@cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

test_all: _test_all clean
_test_all: setup
	cargo test --no-fail-fast -- --nocapture --test-threads=1

end_to_end: _end_to_end clean
_end_to_end: setup
	cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

kafka: _kafka clean
_kafka: setup
	cargo test --test end_to_end test_kafka --features debug --no-fail-fast -- --nocapture --test-threads=1

# With the `debug` feature, the database is not cleaned up after running the test,
# so it can be inspected with psql. Run with:
# TEST=<name> make debug_test
debug_test: setup
	cargo test "$(TEST)" --features debug --no-fail-fast -- --nocapture --test-threads=1

setup:
	@echo "Starting Postgres docker container..."
	docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@echo; sleep 5
	cargo build --workspace --tests
	@echo; echo "Loading DB schema..."; echo
	@target/debug/prepare_postgres

clean:
	@echo "Stopping Postgres container..."
	docker stop lard_tests
	@echo "Removing Postgres container..."
	docker rm lard_tests
