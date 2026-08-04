# kafka-wire-core

Sans-I/O primitives shared by the `kafka-wire` runtime crates.

This crate owns Kafka API versions and ranges, bounded encoders and decoders,
protocol strings, UUIDs, bytes, and unknown tagged fields. It contains no Kafka
API message names, networking, filesystem, process, thread, or async-runtime
policy.

Most applications should use `kafka-wire` or a higher-level Kafka client. See
the [`kafka-wire` repository](https://github.com/kafkars/wire) for the
architecture and compatibility contract.

Licensed under Apache-2.0.
