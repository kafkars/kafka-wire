# Changelog

All notable changes to the public Rust packages are recorded here. This project
uses semantic versioning after publication; unreleased APIs may still change
while the 0.1 beta is being qualified.

## Unreleased

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
