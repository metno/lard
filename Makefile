unit_tests: _unit_tests 
_unit_tests: unit_setup
	cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

test_all: _test_all clean
_test_all: pg_setup
	cargo test --no-fail-fast --workspace -- --nocapture --test-threads=1

end_to_end: _end_to_end clean
_end_to_end: pg_setup
	cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

kafka: _kafka clean
_kafka: setup
	cargo test --test end_to_end test_kafka --features debug --no-fail-fast -- --nocapture --test-threads=1

# With the `debug` feature, the database is not cleaned up after running the test,
# so it can be inspected with psql. Run with:
# TEST=<name> make debug_test
debug_test: pg_setup
	cargo test "$(TEST)" --features debug --no-fail-fast -- --nocapture --test-threads=1

pg_setup: unit_setup
	@echo "Starting Postgres docker container..."
	docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@echo; sleep 5
	@echo; echo "Loading DB schema..."; echo
	@target/debug/prepare_postgres

unit_setup:
	cargo build --workspace --tests

clean:
	@echo "Stopping Postgres container..."
	docker stop lard_tests
	@echo "Removing Postgres container..."
	docker rm lard_tests
