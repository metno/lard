run_tests:
	@echo "Starting Postgres docker container..."
	docker run --name sql-data-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
	@sleep 5
	cargo build --workspace --tests
	cd ..; target/debug/prepare_db
	cargo test --no-fail-fast -- --nocapture --test-threads=1
	make clean

clean: 
	@echo "Stopping Postgres container..."
	docker stop sql-data-test
	@echo "Removing Postgres container..."
	docker rm sql-data-test
