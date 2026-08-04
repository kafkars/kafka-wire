# kafka-wire

`kafka-wire` is a generated, version-aware implementation of the Apache Kafka
wire format for Rust. It owns Kafka message schemas, request/response pairing,
headers, version ranges, bounded encoding and decoding, and RecordBatch v2 byte
mechanics. It performs no networking and depends on no async runtime.

The repository publishes three runtime crates:

- `kafka-wire`: generated Kafka request and response messages behind one flat
  crate facade;
- `kafka-wire-core`: API-independent versions, strings, UUIDs, bounded codecs,
  and tagged-field primitives; and
- `kafka-wire-records`: bounded RecordBatch v2 encoding, decoding, and
  compression.

The generated corpus is pinned to an exact Apache Kafka source revision. Every
generated file carries provenance, and the ordinary repository gate verifies
the complete schema classification, Kafka-authored byte vectors, record-batch
corpus, generated-tree identity, formatting, lints, tests, and rustdoc.

## Status

Version 0.1 is a low-level beta intended for Kafka clients and wire tooling.
The generated message structures follow the pinned Kafka schema and may grow
when a later minor release deliberately advances that corpus. `kafka-wire`
does not own sockets, request correlation, routing, retries, or deadlines; a
driver such as `kafka-driver` owns those policies.

## License

Apache-2.0. Apache Kafka is a trademark of the Apache Software Foundation. This
project is independent and is not endorsed by the Apache Software Foundation.
