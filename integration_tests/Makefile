test: setup
	cargo test --no-fail-fast -- --nocapture --test-threads=1
	@make clean

end-to-end: setup
	cargo test --test end-to-end --no-fail-fast -- --nocapture --test-threads=1
	@make clean

setup:
	@echo "Starting Postgres docker container..."
	docker run --name lard_tests -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@sleep 5
	cargo build --workspace --tests
	cd ..; target/debug/prepare_db

clean:
	@echo "Stopping Postgres container..."
	docker stop lard_tests
	@echo "Removing Postgres container..."
	docker rm lard_tests
