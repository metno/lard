test_all: _test_dummy clean
_test_dummy: setup
	cargo test --no-fail-fast -- --nocapture --test-threads=1

e2e := end_to_end
$(e2e): _e2e_dummy clean
_e2e_dummy: setup
	cargo test --test $(e2e) --no-fail-fast -- --nocapture --test-threads=1

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
