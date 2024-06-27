test_all: _test_all clean
_test_all: setup
	cargo test --no-fail-fast -- --nocapture --test-threads=1

end_to_end: _end_to_end clean
_end_to_end: setup
	cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

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
