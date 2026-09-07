# Releasing kafka-wire

Release `kafka-wire-core`, then `kafka-wire` and `kafka-wire-records`, at the same
version. Only those three packages are published. Keep corpus advances and
performance work outside a release freeze unless a wire-owned defect requires
a fix. The `0.1.x` promise is specified in [CONTRACT.md](CONTRACT.md).

1. Start a release branch from current `origin/main`. Review the contract and
   changelog, update the workspace version, sibling requirements, both lockfiles,
   and the consumer fixture's exact versions together. Preserve the pinned Rust
   compiler and Kafka revision. Run `cargo xtask generate` because the generated
   manifest records generator inputs, including workspace package metadata.
2. Run `just check` and `git diff --check`. Run the maintenance API comparison
   against the current baseline, dependency policy, and the existing fuzz gate.
   Commit with the maintainer's OpenPGP signature and open a focused release PR.
   Require all configured platform and package jobs to pass for that candidate.
3. Run `scripts/package-rc` from the clean release commit. This packages the three
   crates, tests and builds docs from extracted archives, and executes
   `scripts/check-consumer target/package` against those archives. The archive
   stage patches only extracted siblings; the consumer has no checkout runtime
   dependencies. Keep the archives and command logs with their source commit
   and SHA-256 digests. CI retains archives under
   `kafka-wire-release-candidate-<commit>` for 30 days; attach final release
   archives to the GitHub release for durable retention.
4. Merge through repository protection and qualify the exact signed release
   commit with the main-branch workflow. PR archives may record a synthetic
   merge commit. Run `cargo publish --dry-run --locked -p <package>` immediately
   before each matching `cargo publish --locked -p <package>`, waiting for core
   registry availability before the dependent dry-runs and publications.
   Do not bypass required checks or publish a dirty working tree.
5. Run `scripts/check-consumer --registry` after all three versions are visible.
   This copies the same fixture outside the workspace, resolves fresh exact
   crates.io dependencies, and executes it without path dependencies or patches.
   Download the published archives, verify their version and `.cargo_vcs_info`
   commit, rerun extracted-package tests/docs, and retain hashes. Cargo can change
   dependent lockfile registry metadata after a sibling publishes: compare the
   extracted source and provenance, not just prepublication archive bytes.
6. Create and verify the signed `v<version>` tag at the qualified release commit.
   Publish a GitHub release with the final registry archives and hashes. Confirm
   docs.rs builds and all three registry versions are unyanked. The existing
   maintenance job uses the signed stable tag as soon as it exists; before the
   initial tag, only the 0.1.0 candidate may use v0.1.0-rc.3 as its fallback.

The consumer exercises framing and limits, generated field assignment, bounded
message decoding, and RecordBatch round trips through all compression codecs.
It complements the independent Kafka-authored fixture corpus; it is not a broker
qualification harness and does not claim client-level delivery or routing proof.

All scripts honor `TMPDIR`; keep it and Cargo caches/targets on the intended
build volume. `scripts/package-rc` retains archives in `target/package` and
therefore should use the checkout's default Cargo target directory.
