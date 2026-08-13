# kafka-wire

Generated, version-aware Apache Kafka request and response messages for Rust.

The crate exposes generated messages, descriptors, request/response pairing,
whole-request framing, protocol equality, and retained-footprint accounting
through one flat facade. It is sans-I/O: callers own sockets, correlation IDs,
version negotiation, deadlines, and retry policy.

The generated tree is pinned to an exact Apache Kafka source revision and is
qualified against Kafka-authored byte vectors. See the
[`kafka-wire` repository](https://github.com/kafkars/kafka-wire) for provenance,
generation policy, and the complete validation gate.

Licensed under Apache-2.0.
