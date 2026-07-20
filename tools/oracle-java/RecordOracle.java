/*
 * Broker-authored byte oracle for Kafka record batches.
 *
 * The sibling `Oracle.java` answers "what bytes does Kafka write for this
 * message?" by reflecting over the generated `*JsonConverter` classes. It cannot
 * answer the same question about a record batch, and the reason is worth stating
 * because it is what this program exists to fix: a `records` field is opaque to
 * those converters. Kafka's generated reader takes base64 and copies it through
 * untouched, and the writer emits an empty `BinaryNode` and discards the payload.
 * A vector minted that way would only mirror bytes this repository authored,
 * which is weaker than no evidence at all — the misreading would be its own and
 * Kafka would never see it.
 *
 * So this program drives Kafka's own record writers and reports the bytes they
 * produced. Ordinary and compacted non-empty batches go through
 * `MemoryRecordsBuilder`, the same class Kafka's producer uses; empty compacted
 * batches go through `DefaultRecordBatch.writeEmptyHeader`, the writer Kafka
 * provides specifically for that retained header. Every field of the v2 header,
 * the CRC32C over them, the zigzag-varint record framing, and each compression
 * codec's exact framing are then Kafka's statements rather than this repository's.
 *
 * DETERMINISM IS A REQUIREMENT, NOT A CONVENIENCE. Several `MemoryRecords.builder`
 * overloads call `System.currentTimeMillis()` for the log-append timestamp, which
 * would make every refresh rewrite the corpus with new bytes and destroy the
 * signal that a diff carries. This program uses only the fullest overload and
 * passes every field explicitly, including `NO_TIMESTAMP`. `--self-test` proves
 * the property rather than asserting it: it builds each batch twice and refuses
 * to emit anything if the two runs disagree.
 *
 * `--verify` answers the opposite question, and it is the only instrument that
 * can. Compression cannot be judged by byte identity — no two implementations of
 * `Deflater` or zstd make the same internal choices — but byte identity was never
 * the property worth having. The property worth having is that Kafka can READ
 * what this repository wrote, so `--verify` takes bytes produced by the Rust
 * compressor and hands them to `MemoryRecords.readableRecords`, Kafka's own
 * reader, reporting the records it recovered or failing with Kafka's own error.
 *
 * It deliberately owns no vector policy. Which batches are worth covering is
 * decided by `spec/records/plans.json`, and whether this repository agrees with
 * the bytes is decided by `kafka-wire-conformance`.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.DefaultRecordBatch;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

public final class RecordOracle {
    /** Generous enough for any plan case; a batch that overflows it is a plan bug. */
    private static final int BUFFER_BYTES = 1 << 20;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RecordOracle() {
    }

    public static void main(String[] args) {
        try {
            if (args.length == 1 && args[0].equals("--self-test")) {
                selfTest();
                return;
            }
            if (args.length == 1 && args[0].equals("--verify")) {
                verifyAll();
                return;
            }
            if (args.length != 0) {
                throw new OracleException(
                        "usage: RecordOracle [--self-test | --verify] < plans.json > vectors.json");
            }
            encodeAll();
        } catch (Throwable failure) {
            System.err.println("record oracle: " + failure.getMessage());
            System.exit(1);
        }
    }

    /** Read one batch of plan cases from stdin and write their bytes to stdout. */
    private static void encodeAll() throws Exception {
        JsonNode input = MAPPER.readTree(System.in);
        JsonNode batches = required(input, "batches");
        if (!batches.isArray()) {
            throw new OracleException("`batches` must be an array");
        }

        ArrayNode results = MAPPER.createArrayNode();
        for (JsonNode batch : batches) {
            String name = required(batch, "name").asText();
            byte[] once = build(batch);
            byte[] twice = build(batch);
            if (!Arrays.equals(once, twice)) {
                throw new OracleException(name + " did not build deterministically; a vector "
                        + "minted from it would churn on every refresh and its diff would mean nothing");
            }
            ObjectNode result = MAPPER.createObjectNode();
            result.put("name", name);
            result.put("hex", HexFormat.of().formatHex(once));
            results.add(result);
        }

        ObjectNode output = MAPPER.createObjectNode();
        output.set("results", results);
        System.out.println(MAPPER.writeValueAsString(output));
    }

    /** Read foreign batch bytes with Kafka's own reader and report what it found. */
    private static void verifyAll() throws Exception {
        JsonNode input = MAPPER.readTree(System.in);
        JsonNode batches = required(input, "batches");
        if (!batches.isArray()) {
            throw new OracleException("`batches` must be an array");
        }

        ArrayNode results = MAPPER.createArrayNode();
        for (JsonNode batch : batches) {
            String name = required(batch, "name").asText();
            byte[] bytes = HexFormat.of().parseHex(required(batch, "hex").asText());
            ObjectNode result = MAPPER.createObjectNode();
            result.put("name", name);
            try {
                result.set("read", read(bytes));
            } catch (OracleException refusal) {
                throw refusal;
            } catch (Throwable failure) {
                // Kafka's own refusal, reported as Kafka spelled it. A caller
                // that reads "CorruptRecordException" learns which invariant it
                // broke; one that reads "verification failed" learns nothing.
                throw new OracleException(name + ": Kafka could not read these bytes: "
                        + failure.getClass().getName() + ": " + failure.getMessage());
            }
            results.add(result);
        }

        ObjectNode output = MAPPER.createObjectNode();
        output.set("results", results);
        System.out.println(MAPPER.writeValueAsString(output));
    }

    /**
     * Describe every batch and record Kafka recovered from one blob.
     *
     * `ensureValid` is called on the batch and on each record because
     * `readableRecords` is lazy: it wraps the buffer and checks nothing, so a
     * program that only counted batches would report success over a payload with
     * a broken CRC. Iterating a compressed batch is also what decompresses it,
     * which is the step this whole mode exists to put Kafka in charge of.
     */
    private static ObjectNode read(byte[] bytes) {
        ArrayNode batches = MAPPER.createArrayNode();
        for (MutableRecordBatch batch : MemoryRecords.readableRecords(ByteBuffer.wrap(bytes)).batches()) {
            batch.ensureValid();

            ObjectNode described = MAPPER.createObjectNode();
            described.put("compression", batch.compressionType().name);
            described.put("magic", batch.magic());
            described.put("baseOffset", batch.baseOffset());
            described.put("lastOffset", batch.lastOffset());
            described.put("partitionLeaderEpoch", batch.partitionLeaderEpoch());
            described.put("producerId", batch.producerId());
            described.put("producerEpoch", batch.producerEpoch());
            described.put("baseSequence", batch.baseSequence());
            described.put("maxTimestamp", batch.maxTimestamp());
            described.put("timestampType", batch.timestampType().name);
            described.put("transactional", batch.isTransactional());
            described.put("controlBatch", batch.isControlBatch());
            described.set("records", records(batch));
            batches.add(described);
        }

        if (batches.isEmpty()) {
            throw new OracleException("Kafka read no batch at all from these bytes");
        }
        ObjectNode read = MAPPER.createObjectNode();
        read.set("batches", batches);
        return read;
    }

    /** Every record Kafka recovered from one batch, decompressing as it goes. */
    private static ArrayNode records(MutableRecordBatch batch) {
        ArrayNode records = MAPPER.createArrayNode();
        for (Record record : batch) {
            record.ensureValid();
            ObjectNode entry = MAPPER.createObjectNode();
            entry.put("offset", record.offset());
            entry.put("timestamp", record.timestamp());
            entry.put("key", hex(record.key()));
            entry.put("value", hex(record.value()));

            ArrayNode headers = MAPPER.createArrayNode();
            for (Header header : record.headers()) {
                ObjectNode described = MAPPER.createObjectNode();
                described.put("key", header.key());
                described.put("value", hex(header.value()));
                headers.add(described);
            }
            entry.set("headers", headers);
            records.add(entry);
        }
        return records;
    }

    /**
     * Lowercase hex, or JSON null where the record carries no key or value.
     *
     * Hex rather than the base64 the plans are written in, because this file is
     * read back by Rust: `spec/` spells every wire byte string in hex and
     * `kafka-wire-conformance` already owns that conversion, so base64 here would buy
     * a dependency for one field.
     */
    private static String hex(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] out = new byte[buffer.remaining()];
        buffer.duplicate().get(out);
        return HexFormat.of().formatHex(out);
    }

    /** Hex of a header value, which is nullable where a header key is not. */
    private static String hex(byte[] value) {
        return value == null ? null : HexFormat.of().formatHex(value);
    }

    /** Lay out one batch through the Kafka writer that owns its shape. */
    private static byte[] build(JsonNode batch) {
        JsonNode records = required(batch, "records");
        if (!records.isArray()) {
            throw new OracleException("`records` must be an array");
        }
        if (records.isEmpty()) {
            return buildEmpty(batch);
        }

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
        // The fullest overload, with every field supplied. The shorter ones
        // default the log-append timestamp from the wall clock.
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                compression(text(batch, "compression", "none")),
                timestampType(text(batch, "timestampType", "CreateTime")),
                longAt(batch, "baseOffset", 0L),
                RecordBatch.NO_TIMESTAMP,
                longAt(batch, "producerId", RecordBatch.NO_PRODUCER_ID),
                (short) intAt(batch, "producerEpoch", RecordBatch.NO_PRODUCER_EPOCH),
                intAt(batch, "baseSequence", RecordBatch.NO_SEQUENCE),
                boolAt(batch, "transactional"),
                intAt(batch, "partitionLeaderEpoch", RecordBatch.NO_PARTITION_LEADER_EPOCH));

        long offset = longAt(batch, "baseOffset", 0L);
        int index = 0;
        for (JsonNode record : records) {
            // Offsets must increase strictly, so a plan that does not state a
            // delta gets the record's position. Stating one is for the cases
            // that are about the delta itself.
            builder.appendWithOffset(
                    offset + intAt(record, "offsetDelta", index),
                    longAt(record, "timestamp", 0L),
                    bytes(record, "key"),
                    bytes(record, "value"),
                    headers(record));
            index += 1;
        }

        ByteBuffer written = builder.build().buffer();
        byte[] out = new byte[written.remaining()];
        written.get(out);
        return out;
    }

    /** Lay out the header Kafka retains when compaction removes every record. */
    private static byte[] buildEmpty(JsonNode batch) {
        String codec = text(batch, "compression", "none");
        if (!codec.equals("none")) {
            throw new OracleException("an empty retained batch cannot declare compression `" + codec + "`");
        }

        long baseOffset = longAt(batch, "baseOffset", 0L);
        int lastOffsetDelta = required(batch, "lastOffsetDelta").asInt();
        long lastOffset;
        try {
            lastOffset = Math.addExact(baseOffset, lastOffsetDelta);
        } catch (ArithmeticException overflow) {
            throw new OracleException("baseOffset plus lastOffsetDelta overflows int64");
        }

        ByteBuffer buffer = ByteBuffer.allocate(DefaultRecordBatch.RECORD_BATCH_OVERHEAD);
        DefaultRecordBatch.writeEmptyHeader(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                longAt(batch, "producerId", RecordBatch.NO_PRODUCER_ID),
                (short) intAt(batch, "producerEpoch", RecordBatch.NO_PRODUCER_EPOCH),
                intAt(batch, "baseSequence", RecordBatch.NO_SEQUENCE),
                baseOffset,
                lastOffset,
                intAt(batch, "partitionLeaderEpoch", RecordBatch.NO_PARTITION_LEADER_EPOCH),
                timestampType(text(batch, "timestampType", "CreateTime")),
                longAt(batch, "maxTimestamp", RecordBatch.NO_TIMESTAMP),
                boolAt(batch, "transactional"),
                boolAt(batch, "control"));
        buffer.flip();
        byte[] out = new byte[buffer.remaining()];
        buffer.get(out);
        return out;
    }

    /**
     * Prove the determinism claim, and that every codec this corpus names is
     * actually reachable on the classpath.
     *
     * snappy, lz4, and zstd are not bundled in the clients jar. Without them a
     * refresh would fail late and confusingly, so the failure is moved here where
     * it names the missing codec.
     */
    private static void selfTest() throws Exception {
        for (String codec : new String[] {"none", "gzip", "snappy", "lz4", "zstd"}) {
            ObjectNode batch = MAPPER.createObjectNode();
            batch.put("name", codec);
            batch.put("compression", codec);
            ArrayNode records = MAPPER.createArrayNode();
            ObjectNode record = MAPPER.createObjectNode();
            record.put("timestamp", 1_700_000_000_000L);
            record.put("value", "aGVsbG8=");
            records.add(record);
            batch.set("records", records);

            byte[] once = build(batch);
            byte[] twice = build(batch);
            if (!Arrays.equals(once, twice)) {
                throw new OracleException(codec + " built two different batches from one plan");
            }
            System.out.printf("%-7s reachable, %d byte(s), deterministic across two builds%n",
                    codec, once.length);
        }
        System.out.println("every codec this corpus names is on the classpath and builds reproducibly.");
    }

    private static Compression compression(String name) {
        return switch (name) {
            case "none" -> Compression.NONE;
            case "gzip" -> Compression.gzip().build();
            case "snappy" -> Compression.snappy().build();
            case "lz4" -> Compression.lz4().build();
            case "zstd" -> Compression.zstd().build();
            default -> throw new OracleException("unknown compression `" + name + "`");
        };
    }

    private static TimestampType timestampType(String name) {
        return switch (name) {
            case "CreateTime" -> TimestampType.CREATE_TIME;
            case "LogAppendTime" -> TimestampType.LOG_APPEND_TIME;
            default -> throw new OracleException("unknown timestampType `" + name + "`");
        };
    }

    private static Header[] headers(JsonNode record) {
        JsonNode declared = record.get("headers");
        if (declared == null || declared.isNull()) {
            return new Header[0];
        }
        List<Header> headers = new ArrayList<>();
        for (JsonNode header : declared) {
            byte[] value = bytes(header, "value");
            headers.add(new RecordHeader(required(header, "key").asText(), value));
        }
        return headers.toArray(new Header[0]);
    }

    /** A base64 field, or null where the record carries no key or value. */
    private static byte[] bytes(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) {
            return null;
        }
        return java.util.Base64.getDecoder().decode(value.asText().getBytes(StandardCharsets.UTF_8));
    }

    private static String text(JsonNode node, String field, String fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : value.asText();
    }

    private static long longAt(JsonNode node, String field, long fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : value.asLong();
    }

    private static int intAt(JsonNode node, String field, int fallback) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? fallback : value.asInt();
    }

    private static boolean boolAt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && value.asBoolean();
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null) {
            throw new OracleException("missing required field `" + field + "`");
        }
        return value;
    }

    /** A refusal this program is responsible for, as opposed to a Kafka error. */
    private static final class OracleException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        OracleException(String message) {
            super(message);
        }
    }
}
