# Run unit tests (lib only, no integration tests)
test:
    cargo nextest run --all-features --lib

# Run integration tests
test-integration:
    cargo nextest run --all-features --test integration --test high_level --test recovery

# Run all tests
test-all:
    cargo nextest run --all-features
