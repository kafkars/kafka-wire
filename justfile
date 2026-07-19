set shell := ["bash", "-euo", "pipefail", "-c"]

default: check

generate:
    cargo xtask generate

generated-check:
    cargo xtask generated-check


# Verify the broker-authored byte vectors. Pure Rust: no Java, no jar, no network.
vectors-check:
    cargo xtask vectors --check

# Re-author them from the pinned Apache Kafka jar. Needs Java and the jar named
# by spec/oracle.lock; run by a human on purpose, never by CI.
vectors-refresh:
    cargo xtask vectors --refresh

fmt:
    cargo fmt --all --check

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace --all-features

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

check: generated-check vectors-check fmt lint test doc

tree:
    find crates xtask spec -type f | sort
