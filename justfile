set shell := ["bash", "-euo", "pipefail", "-c"]

default: check

generate:
    cargo xtask generate

generated-check:
    cargo xtask generated-check


fmt:
    cargo fmt --all --check

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
    cargo test --workspace --all-features

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

check: generated-check fmt lint test doc

tree:
    find crates xtask spec -type f | sort
