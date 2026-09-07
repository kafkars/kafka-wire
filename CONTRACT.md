# Runtime contract for 0.1.x

This contract covers `kafka-wire-core`, `kafka-wire`, and `kafka-wire-records`
starting at 0.1.0. Patch releases preserve their public Rust API and documented
wire behavior. The unpublished schema, generator, conformance, and xtask crates
are repository tools, not supported dependency packages. Rust 1.88 is the
minimum compiler for this line; none of the runtime crates has feature flags.

## Supported surface

| Package | Supported responsibilities |
| --- | --- |
| `kafka-wire-core` | Versions and ranges, strings, UUIDs, tagged fields, bytes, bounded decoding, encoding targets, and the encode/decode traits. |
| `kafka-wire` | The flat public facade of generated messages and nested types, API/version descriptors, request/response pairings, headers, consumer-protocol payload helpers, request framing and measurement, protocol equality, and retained-footprint accounting. |
| `kafka-wire-records` | RecordBatch v2, records and headers, CRC32C validation, strict and partial-tail decoding, bounded encoding, and uncompressed, gzip, LZ4, Snappy, and Zstandard batches. |

Generated version ranges describe the pinned schema corpus, not a promise that
every broker supports every API. Callers choose a supported version explicitly;
unsupported versions are errors. Unknown flexible tagged fields are preserved,
subject to decode limits and validation of tag order and collisions with known
tags. Non-default fields absent in the selected version are rejected on encode.
The generator and corpus provenance remain checked by the existing gates.

Sockets, response-frame assembly, correlation allocation and matching, version
negotiation, routing, retries, deadlines, consumer-group and transaction policy
belong to callers. Having DTOs for an operation does not implement that policy.
RecordBatch v2 support does not imply support for older message-set formats.

## Public type evolution

- Generated messages, headers, and nested structs are `#[non_exhaustive]`.
  Construct them with `Default::default()` and assign public fields. Destructure
  with `..`; external struct literals and struct-update construction are not
  supported for these types.
- New fields may be added to those non-exhaustive types in a patch only when
  existing field names, types, visibility, trait implementations, and behavior
  at already-supported protocol versions remain compatible. New-version fields
  must default to an encodable value for old versions. Turning a formerly
  unknown tag into a known field also needs review of round-trip behavior and
  collision handling; a schema refresh is not automatically compatible.
- Existing protocol defaults, version ranges, wire bytes, and public type names
  are part of the baseline. A wire-correctness fix must be identified explicitly
  in the changelog and backed by independent conformance evidence.
- Handwritten public structs that allow literals today, including `Record`,
  `RecordHeader`, `RecordBatch`, and `RequestFrameMeasure`, retain that ability.
  Adding required fields or adding `#[non_exhaustive]` would break this line.
- `DecodeError`, `EncodeError`, `TaggedFieldsError`, `RecordError`, and
  `RecordBatchDecode` are already non-exhaustive. Downstream matches need a
  fallback; new cases may be added. Existing exhaustive enums, including
  `Compression`, `TimestampType`, `MessageDirection`, `TagOutcome`, and
  `PremeasuredWrite`, keep their variants within 0.1.x.
- Existing public traits remain implementable under their current bounds;
  required methods or new supertraits need an incompatible release. Internal
  module paths, generated filenames, Rust memory layout, diagnostic wording,
  and compressed byte identity are not compatibility promises. Decoded content
  and Kafka-compatible compression framing are.

The existing cargo-semver-checks gate protects all three runtime crates. The
0.1.0 candidate is compared with v0.1.0-rc.3 using patch-level checks; once the
signed v0.1.0 tag exists it becomes the baseline for this line. Semantic and
protocol compatibility also require the byte vectors, record fixtures, tests,
and review; an API checker alone cannot establish them.

## Limits and failure behavior

`DecodeLimits` bounds the input accepted by a decoder, individual strings and
byte fields, collection counts, and unknown tags. These are per-container
parse-time limits, not a cumulative process-memory ceiling. Generated
`RetainedSize` reports recursive container capacity and visible byte spans after
construction or decoding. Shared backing allocations may be larger than their
visible spans; callers own aggregate admission and retained-resource policy.

`OutboundFrameLimits` counts header plus body bytes, excluding the four-byte
length prefix. `measure_request` reports the full length including that prefix
and the response header version. Both sizing and encoding reject frames beyond
the caller limit or Kafka's signed 32-bit prefix before reserving output.

Record limits independently bound the complete encoded batch, expanded payload,
record/header counts, and fields. The expanded-payload limit also determines the
accepted Zstandard history window, rounded up to a power of two within the
codec's range. A small payload can still be rejected if its frame advertises a
larger window; use a budget suitable for both the output and the codec window. Record decoding overrides the inner wire
frame limit with the already-bounded container size. `decode_next` also accepts
a cumulative remaining budget for visible payload newly retained after
decompression; uncompressed slices keep the caller's input allocation.

Complete-message `decode_from_bytes` rejects trailing bytes. Low-level decoder
operations and `KafkaDecode::decode` may advance their decoder before an error;
discard that decoder after failure. `RecordBatch::decode` and `decode_next`
advance their input only for a complete, successfully decoded batch. The strict
entry point rejects an incomplete tail; `decode_next` reports `PartialTrailing`
without advancing it. Corruption remains an error, not an empty record set.

`KafkaEncode::encode_into`, `encode_request`, and `RecordBatch::encode_into`
restore the destination's previous length and bytes on returned errors;
capacity need not shrink. Low-level `Encoder` writes and `KafkaEncode::encode`
do not promise rollback. Match typed errors for unsupported versions, invalid
values, corruption, and exceeded limits; their display strings are diagnostic.
These wire results say nothing about network submission or delivery certainty.
