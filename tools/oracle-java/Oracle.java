/*
 * Broker-authored byte oracle for the Kafka protocol test corpus.
 *
 * This program owns exactly one question: given a message name, a version, and a
 * canonical JSON value, what bytes does Apache Kafka's own generated Java writer
 * produce? It answers by reflecting over the `<Message>DataJsonConverter` classes
 * that ship inside the `clients` jar and serializing through `MessageUtil`, so the
 * answer is authored by the code brokers actually run rather than by any Rust in
 * this repository.
 *
 * It deliberately owns no vector policy: it does not choose which cases are worth
 * covering, does not read or write the `spec/vectors` tree, and does not know that
 * a Rust implementation exists. Those decisions belong to `xtask/src/vectors.rs`.
 *
 * THE VERSION GUARD IS THE POINT. Kafka's generated `write` methods gate every
 * field with `if (_version >= N)` and never check an upper bound. Asked to write a
 * message at a version the jar does not know, Java emits the highest layout it
 * does know and returns normally. Those bytes look authoritative and are wrong.
 * `requireSupportedVersion` is the only thing standing between that behaviour and
 * a checked-in vector, so it runs before the converter is ever invoked, and
 * `--self-test` proves both that Java accepts the out-of-range version and that
 * this program refuses it.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;

import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

public final class Oracle {
    /** Package holding every generated `<Message>Data` and its JSON converter. */
    private static final String MESSAGE_PACKAGE = "org.apache.kafka.common.message";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Oracle() {
    }

    public static void main(String[] args) {
        try {
            if (args.length == 1 && args[0].equals("--self-test")) {
                selfTest(System.out);
                return;
            }
            if (args.length != 0) {
                throw new OracleException("usage: Oracle [--self-test] < plan.json > result.json");
            }
            encodeAll();
        } catch (Throwable failure) {
            System.err.println("oracle: " + failure.getMessage());
            System.exit(1);
        }
    }

    /**
     * Read one encoding batch from stdin and write its answers to stdout.
     *
     * A batch rather than a single triple keeps one JVM start per refresh, and
     * keeps the answers in the order the caller asked for them.
     */
    private static void encodeAll() throws Exception {
        JsonNode input = MAPPER.readTree(System.in);
        JsonNode requests = required(input, "requests");
        if (!requests.isArray()) {
            throw new OracleException("`requests` must be an array");
        }

        ArrayNode results = MAPPER.createArrayNode();
        for (JsonNode request : requests) {
            String message = required(request, "message").asText();
            short version = versionOf(request);
            JsonNode value = required(request, "json_value");

            Encoded encoded = encode(message, version, value, request.get("unknown_tagged_fields"));

            ObjectNode result = MAPPER.createObjectNode();
            result.put("message", message);
            result.put("version", version);
            result.put("api_key", encoded.apiKey);
            result.put("hex", encoded.hex);
            results.add(result);
        }

        ObjectNode output = MAPPER.createObjectNode();
        output.set("results", results);
        System.out.println(MAPPER.writeValueAsString(output));
    }

    /**
     * Serialize one message and report the bytes Kafka's own writer produced.
     *
     * The version guard runs against a freshly constructed data object before the
     * converter sees the JSON, so an out-of-range request is refused before any
     * field is populated and no partial answer can escape.
     */
    private static Encoded encode(String message, short version, JsonNode value, JsonNode taggedFields)
            throws Exception {
        Message data = newData(message);
        requireSupportedVersion(message, data, version);

        Message populated = readConverter(message, value, version);
        applyUnknownTaggedFields(populated, taggedFields);

        ByteBuffer buffer = MessageUtil.toByteBufferAccessor(populated, version).buffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);

        short apiKey = populated instanceof ApiMessage apiMessage ? apiMessage.apiKey() : -1;
        return new Encoded(apiKey, HexFormat.of().formatHex(bytes));
    }

    /**
     * Reject a version the jar's own message definition does not declare.
     *
     * Kafka's `write` never performs this check; see the file contract above.
     */
    private static void requireSupportedVersion(String message, Message data, short version) {
        short lowest = data.lowestSupportedVersion();
        short highest = data.highestSupportedVersion();
        if (version < lowest || version > highest) {
            throw new OracleException(String.format(
                    "%s does not support version %d; this jar declares %d-%d. "
                            + "Kafka's writer would silently emit the nearest layout it knows "
                            + "instead of failing, so refusing here is the only thing preventing "
                            + "a confidently wrong vector.",
                    message, version, lowest, highest));
        }
    }

    private static Message newData(String message) throws Exception {
        Class<?> dataClass = Class.forName(MESSAGE_PACKAGE + "." + message + "Data");
        Object instance = dataClass.getDeclaredConstructor().newInstance();
        if (!(instance instanceof Message data)) {
            throw new OracleException(message + "Data is not a Kafka Message");
        }
        return data;
    }

    private static Message readConverter(String message, JsonNode value, short version) throws Exception {
        Class<?> converter = Class.forName(MESSAGE_PACKAGE + "." + message + "DataJsonConverter");
        Method read = converter.getMethod("read", JsonNode.class, short.class);
        Object instance = read.invoke(null, value, version);
        if (!(instance instanceof Message data)) {
            throw new OracleException(message + "DataJsonConverter.read did not return a Message");
        }
        return data;
    }

    /**
     * Attach unknown tagged fields, which no generated JSON converter can express.
     *
     * A broker forwards tags it does not understand, so a corpus that never
     * carries one would leave the forwarding path untested.
     */
    private static void applyUnknownTaggedFields(Message data, JsonNode taggedFields) {
        if (taggedFields == null || taggedFields.isNull()) {
            return;
        }
        if (!taggedFields.isArray()) {
            throw new OracleException("`unknown_tagged_fields` must be an array");
        }

        List<RawTaggedField> fields = new ArrayList<>();
        for (JsonNode field : taggedFields) {
            int tag = required(field, "tag").asInt();
            byte[] payload = HexFormat.of().parseHex(required(field, "data_hex").asText());
            fields.add(new RawTaggedField(tag, payload));
        }
        data.unknownTaggedFields().addAll(fields);
    }

    /**
     * Demonstrate the hazard and prove the guard fires against it.
     *
     * Reported to stdout rather than merely asserted, so the refresh that runs
     * this leaves evidence a reviewer can read.
     */
    private static void selfTest(PrintStream out) throws Exception {
        String message = "ApiVersionsRequest";
        ObjectNode value = MAPPER.createObjectNode();
        value.put("clientSoftwareName", "guard");
        value.put("clientSoftwareVersion", "1");
        value.putNull("clusterId");
        value.put("nodeId", -1);

        short highest = newData(message).highestSupportedVersion();
        short beyond = (short) (highest + 1);

        String unguarded = unguardedHex(message, value, beyond);
        String atHighest = encode(message, highest, value, null).hex;
        if (unguarded == null) {
            throw new OracleException(
                    "self-test could not establish the hazard: Kafka refused version " + beyond
                            + " on its own, so this test no longer proves what the guard is for");
        }
        out.printf("unguarded v%d encoded %d byte(s) with no error: %s%n",
                beyond, unguarded.length() / 2, unguarded);
        out.printf("guarded   v%d encoded %d byte(s): %s%n",
                highest, atHighest.length() / 2, atHighest);
        if (!unguarded.equals(atHighest)) {
            throw new OracleException(
                    "self-test expected the silent downgrade to reproduce the v" + highest
                            + " layout; got " + unguarded + " versus " + atHighest);
        }
        out.println("hazard confirmed: an unguarded out-of-range version yields "
                + "in-range bytes that would look authoritative.");

        requireRefusal(message, value, beyond);
        requireRefusal(message, value, (short) (newData(message).lowestSupportedVersion() - 1));
        out.println("guard fires on both the version above the ceiling and the version "
                + "below the floor; in-range versions still encode.");
    }

    /** Serialize without the guard, or report that Kafka refused on its own. */
    private static String unguardedHex(String message, JsonNode value, short version) throws Exception {
        try {
            Message data = readConverter(message, value, version);
            ByteBuffer buffer = MessageUtil.toByteBufferAccessor(data, version).buffer();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return HexFormat.of().formatHex(bytes);
        } catch (RuntimeException refused) {
            return null;
        }
    }

    private static void requireRefusal(String message, JsonNode value, short version) throws Exception {
        try {
            encode(message, version, value, null);
        } catch (OracleException expected) {
            return;
        }
        throw new OracleException(
                "self-test FAILED: version " + version + " of " + message
                        + " was encoded despite being outside the jar's declared range");
    }

    private static short versionOf(JsonNode request) {
        int version = required(request, "version").asInt();
        if (version < Short.MIN_VALUE || version > Short.MAX_VALUE) {
            throw new OracleException("version " + version + " does not fit a Kafka int16 version");
        }
        return (short) version;
    }

    private static JsonNode required(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null) {
            throw new OracleException("missing required field `" + field + "`");
        }
        return value;
    }

    /** One message's API key and the bytes Kafka wrote for it. */
    private record Encoded(short apiKey, String hex) {
    }

    /** A refusal this program is responsible for, as opposed to a Kafka error. */
    private static final class OracleException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        OracleException(String message) {
            super(message);
        }
    }
}
