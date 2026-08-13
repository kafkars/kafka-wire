# kafka-wire-records

Bounded Kafka RecordBatch v2 encoding, decoding, and compression for Rust.

The crate owns record and batch layout, CRC32C validation, decode budgets, and
gzip, LZ4, Snappy, and Zstandard framing. It is separate from generated Kafka
API messages so callers that only need log-record mechanics do not acquire the
complete protocol corpus.

See the [`kafka-wire` repository](https://github.com/kafkars/kafka-wire) for the
Kafka-authored conformance corpus and validation policy.

Licensed under Apache-2.0.
