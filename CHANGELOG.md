# Changelog

All notable changes to the public Rust packages are recorded here. The public
surfaces of the three runtime crates follow the supported `0.1.x` compatibility
contract in [CONTRACT.md](CONTRACT.md).

## Unreleased

## 0.1.0 - 2026-09-07

### Changed

- Include the Apache Kafka attribution notice in the generated wire package
  and verify its exact contents during archive qualification.
- Establish the supported `0.1.x` contract for codecs, generated messages,
  framing, record batches, compression, limits, and errors. Runtime behavior
  and the pinned Kafka schema corpus are unchanged from 0.1.0-rc.3.
- Clarify that the Zstandard decode budget also bounds the advertised history
  window, which can exceed a small frame's decoded payload.
- Document compatible generated-field evolution and preserve existing public
  struct construction and exhaustive enum matching.
- Qualify an independent consumer against extracted release archives and reuse
  it for registry-only verification after publication.
- Advance API compatibility protection to the final RC for this release and
  the signed 0.1.0 baseline once its tag exists.

## 0.1.0-rc.3 - 2026-08-25

### Added

- Exact request-frame premeasurement for transport-capacity reservation.

## 0.1.0-rc.2 - 2026-08-13

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
