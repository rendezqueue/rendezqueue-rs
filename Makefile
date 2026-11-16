CARGO ?= cargo

# Keep formatting and checks sequential, even with multiple requested goals.
.NOTPARALLEL:
.PHONY: tidytest tidy lint test
tidytest:
	$(MAKE) tidy
	$(MAKE) lint
	$(MAKE) test

tidy:
	$(CARGO) fmt --all

lint:
	$(CARGO) fmt --all --check
	$(CARGO) clippy --all-targets -- -D warnings

test:
	$(CARGO) test --quiet
