# Changelog

All notable changes to the public Rust packages are recorded here. The public
surfaces of the three runtime crates are compatibility-protected against
v0.1.0-rc.1; compatible additions may still land before 0.1.0.

## Unreleased

### Added

- Kafka-authored control and delete-horizon RecordBatch fixtures.
- Checked RecordBatch fuzz seeds, fuzz-target builds, and scheduled smoke
  campaigns.
- Dependency, license, advisory, source, duplicate-version, and public API
  compatibility policy in hosted maintenance checks.

### Changed

- Release qualification tests and documents each extracted public crate and
  retains the exact `.crate` archives as workflow artifacts.

### Fixed

- Resource-limit documentation now distinguishes parse-time bounds from
  post-decode retained-footprint accounting.
- Nullable-array documentation now matches the actual nullable-length contract.

### Security

- Security guidance now reflects the public release candidate and its private
  vulnerability-reporting path.

## 0.1.0-rc.1 - 2026-08-13

### Added

- Version-aware generated Kafka request and response types behind one flat
  `kafka-wire` facade.
- Sans-I/O bounded primitives in `kafka-wire-core`.
- Bounded RecordBatch v2 encoding, decoding, validation, and gzip, LZ4,
  Snappy, and Zstandard compression in `kafka-wire-records`.
- Pinned Apache Kafka schema provenance, Kafka-authored byte vectors, generated
  tree identity, and ordinary conformance gates.

### Security

- Decode and retained-size budgets fail closed on hostile lengths and nested
  allocation shapes; runtime crates own no sockets, processes, or async
  runtime capability.
