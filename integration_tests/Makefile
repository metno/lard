# Basically the same as defining all targets .PHONY
MAKEFLAGS += --always-make
	
unit_tests:
	cargo build --workspace --tests
	cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

test_all: --test_all clean
--test_all: setup
	cargo test --workspace --no-fail-fast -- --nocapture --test-threads=1

end_to_end: --end_to_end clean
--end_to_end: setup
	cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

debug_kafka: --kafka clean
--kafka: setup
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
	@cd ..; target/debug/prepare_postgres

clean:
	@echo "Stopping Postgres container..."
	docker stop lard_tests
	@echo "Removing Postgres container..."
	docker rm lard_tests
