# Releasing kafka-wire

Publishing is permanent. Before a release, ensure the repository named by the
manifests is publicly readable, the intended version is committed, and the
complete repository gate passes with the pinned Rust toolchain:

```sh
just check
```

The runtime packages must be published in dependency order:

```sh
cargo publish -p kafka-wire-core
# Wait until crates.io serves kafka-wire-core at the released version.
cargo publish -p kafka-wire
cargo publish -p kafka-wire-records
```

Run `cargo publish --dry-run -p <package>` immediately before each matching
publish command. The dependent dry-runs intentionally occur only after
`kafka-wire-core` is visible in the registry, proving the packaged manifest
resolves without local path help.

After publishing, verify all three docs.rs builds and create the matching Git
tag. Never use `--allow-dirty` for a release.
