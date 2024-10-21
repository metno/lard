test_unit:
	cargo build --workspace --tests
	cargo test --no-fail-fast --workspace --exclude lard_tests -- --nocapture

test_all: setup && clean
	cargo test --workspace --no-fail-fast -- --nocapture --test-threads=1

test_end_to_end: setup && clean
	cargo test --test end_to_end --no-fail-fast -- --nocapture --test-threads=1

# Debug commands don't perfom the clean up action after running.
# This allows to manually check the state of the database.
debug_kafka: setup
	cargo test --test end_to_end test_kafka --features debug --no-fail-fast -- --nocapture --test-threads=1

debug_migrations: setup
    @ cd kdvh_importer && go test -v ./...

# just debug_test <test_name>
debug_test TEST: setup
	cargo test {{TEST}} --features debug --no-fail-fast -- --nocapture --test-threads=1

setup:
	@ echo "Starting Postgres docker container..."
	docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@ echo; sleep 5
	cargo build --workspace --tests
	@ echo; echo "Loading DB schema..."; echo
	@target/debug/prepare_postgres

clean:
	@ echo "Stopping Postgres container..."
	docker stop lard_tests
	@ echo "Removing Postgres container..."
	docker rm lard_tests
