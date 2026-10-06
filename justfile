# Run unit tests (lib only, no integration tests)
test:
    cargo nextest run --lib

# Run integration tests
test-integration:
    cargo nextest run --test integration --test high_level

# Run all tests
test-all:
    cargo nextest run
