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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.utils.internals.ImplicitLinkedHashCollection;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

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
            if (args.length == 1 && args[0].equals("--defaults")) {
                reportDefaults();
                return;
            }
            if (args.length != 0) {
                throw new OracleException(
                        "usage: Oracle [--self-test | --defaults] < plan.json > result.json");
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

    /**
     * Report the default every field of every named message carries.
     *
     * This is the second question this program answers, and it exists because the
     * byte corpus structurally cannot reach it. A vector proves that decoding
     * Kafka's bytes and re-encoding them reproduces those bytes; when a field is
     * absent from a version, the decoder substitutes a default and the encoder
     * then compares against that same default and writes nothing. A wrong default
     * agrees with itself perfectly and the round trip stays green.
     *
     * Kafka's generated `<Message>Data` initializes every field to the default its
     * schema declares, so a freshly constructed instance *is* upstream's default
     * table. Reading it here compares this repository's lowering against Kafka's
     * own generator — two independent readings of one schema — rather than against
     * itself. The implicit defaults matter most: where upstream declares no
     * `"default"`, both sides invent one from the field's type, and nothing in the
     * schema says they have to agree.
     */
    private static void reportDefaults() throws Exception {
        JsonNode input = MAPPER.readTree(System.in);
        JsonNode messages = required(input, "messages");
        if (!messages.isArray()) {
            throw new OracleException("`messages` must be an array");
        }

        ArrayNode reported = MAPPER.createArrayNode();
        for (JsonNode name : messages) {
            String message = name.asText();
            ObjectNode entry = MAPPER.createObjectNode();
            entry.put("message", message);
            entry.set("structs", structsOf(message));
            reported.add(entry);
        }

        ObjectNode output = MAPPER.createObjectNode();
        output.set("messages", reported);
        System.out.println(MAPPER.writeValueAsString(output));
    }

    /**
     * Every struct one message declares, keyed the way this repository names them.
     *
     * Kafka emits a message's structs as nested classes of its `Data` class, so
     * the walk is over declared classes rather than over field values: a struct
     * reached through an array would otherwise be unreachable, because a
     * default-constructed message holds that array empty and there is no element
     * to inspect.
     *
     * The top-level entry is keyed by the message name rather than by the Java
     * class name, because `Data` is Kafka's suffix and not part of the protocol.
     * Nested structs keep upstream's own spelling, which is exactly the scope
     * their message module supplies: unique within that message, and not beyond it.
     */
    private static ArrayNode structsOf(String message) throws Exception {
        ArrayNode structs = MAPPER.createArrayNode();
        Class<?> root = generated(message, "");

        Deque<Class<?>> pending = new ArrayDeque<>();
        Set<Class<?>> seen = new HashSet<>();
        pending.add(root);
        while (!pending.isEmpty()) {
            Class<?> current = pending.poll();
            if (!seen.add(current)) {
                continue;
            }
            for (Class<?> nested : current.getDeclaredClasses()) {
                pending.add(nested);
            }
            // A `*Collection` is Kafka's own list container, not a protocol
            // struct; it carries no declared field and must not become one.
            if (!Message.class.isAssignableFrom(current)
                    || Collection.class.isAssignableFrom(current)) {
                continue;
            }

            ObjectNode struct = MAPPER.createObjectNode();
            struct.put("struct", current == root ? message : current.getSimpleName());
            struct.set("fields", fieldsOf(current));
            structs.add(struct);
        }
        return structs;
    }

    /** One struct's declared fields and the value Kafka initializes each to. */
    private static ArrayNode fieldsOf(Class<?> struct) throws Exception {
        Constructor<?> constructor = struct.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object instance = constructor.newInstance();

        ArrayNode fields = MAPPER.createArrayNode();
        for (Field field : struct.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
                continue;
            }
            String name = field.getName();
            if (name.equals("_unknownTaggedFields")) {
                continue;
            }
            if (isCollectionBookkeeping(struct, field)) {
                continue;
            }

            field.setAccessible(true);
            ObjectNode reported = MAPPER.createObjectNode();
            reported.put("field", name);
            reported.put("java_type", field.getType().getSimpleName());
            reported.set("default", describe(struct, name, field.get(instance)));
            fields.add(reported);
        }
        return fields;
    }

    /**
     * Whether a field is `ImplicitLinkedHashCollection`'s intrusive list, not protocol.
     *
     * A struct held in one of Kafka's own collections carries two `int` fields
     * named `next` and `prev`, initialized to -2. They are bookkeeping for the
     * container and appear in no schema. Reporting them would invent a field on
     * every collection-held struct in the corpus.
     *
     * The test is the interface that owns them rather than the names alone, and a
     * `next` or `prev` found anywhere else is refused rather than skipped: this
     * program may not decide that something upstream declared is uninteresting,
     * and today no schema declares either name.
     */
    private static boolean isCollectionBookkeeping(Class<?> struct, Field field) {
        String name = field.getName();
        if (!name.equals("next") && !name.equals("prev")) {
            return false;
        }
        boolean intrusive = ImplicitLinkedHashCollection.Element.class.isAssignableFrom(struct)
                && field.getType() == int.class;
        if (!intrusive) {
            throw new OracleException(struct.getSimpleName() + "." + name
                    + " is named like collection bookkeeping but is not an int on an "
                    + "ImplicitLinkedHashCollection.Element. Skipping it would drop a real "
                    + "protocol field; this rule was written when no schema declared either name");
        }
        return true;
    }

    /**
     * Render one default as a kind-tagged value the Rust side can compare against.
     *
     * Tagged by kind rather than emitted as a bare JSON value because the
     * distinctions that matter here are exactly the ones bare JSON erases: an
     * absent bytes field and an empty one are both plausible, and `null` for a
     * string is a different claim than `""`.
     */
    private static JsonNode describe(Class<?> struct, String field, Object value) {
        ObjectNode described = MAPPER.createObjectNode();
        if (value == null) {
            described.put("kind", "null");
            return described;
        }
        switch (value) {
            case Boolean literal -> {
                described.put("kind", "bool");
                described.put("value", literal);
            }
            case Byte literal -> integer(described, literal.longValue());
            case Short literal -> integer(described, literal.longValue());
            case Integer literal -> integer(described, literal.longValue());
            case Long literal -> integer(described, literal);
            case Double literal -> {
                described.put("kind", "float");
                described.put("value", literal);
            }
            case String literal -> {
                described.put("kind", "string");
                described.put("value", literal);
            }
            case Uuid literal -> {
                described.put("kind", "uuid");
                // Kafka spells a uuid base64url without padding, in the schemas
                // and here alike; `toString` is that spelling.
                described.put("value", literal.toString());
            }
            case byte[] literal -> emptyOrRefuse(described, struct, field, literal.length);
            case ByteBuffer literal -> emptyOrRefuse(described, struct, field, literal.remaining());
            case Collection<?> literal -> emptyOrRefuse(described, struct, field, literal.size());
            case Message ignored -> described.put("kind", "struct");
            default -> throw new OracleException(struct.getSimpleName() + "." + field
                    + " defaults to a " + value.getClass().getName()
                    + ", which this program has no rule for");
        }
        return described;
    }

    private static void integer(ObjectNode described, long value) {
        described.put("kind", "int");
        described.put("value", value);
    }

    /**
     * Record an empty container, refusing a populated one.
     *
     * Kafka's generator permits only empty or null as the default of an array,
     * bytes, or records field, so a populated container here means the rule this
     * program reads by no longer holds.
     */
    private static void emptyOrRefuse(ObjectNode described, Class<?> struct, String field, int size) {
        if (size != 0) {
            throw new OracleException(struct.getSimpleName() + "." + field
                    + " defaults to a container holding " + size + " element(s); Kafka's own "
                    + "generator admits only an empty or null default here");
        }
        described.put("kind", "empty");
    }

    /**
     * Resolve one generated class, which Kafka names differently by schema type.
     *
     * A request, response, or header becomes `<name>Data`; a `"type": "data"` schema —
     * AbortedTxn, LeaderChangeMessage, VotersRecord and the rest of the record-adjacent
     * set — keeps its own name. This program is handed a message name and not a schema
     * type, so it asks the jar rather than the caller, preferring the suffixed name
     * because that is what an API message uses.
     *
     * Resolving BOTH is refused rather than chosen between. Measured over all 201 pinned
     * schemas the two candidates never both exist, so a jar where they do is a jar this
     * rule was not written for, and picking one there would silently answer a question
     * about the wrong class.
     */
    private static Class<?> generated(String message, String suffix) {
        Class<?> suffixed = lookup(message + "Data" + suffix);
        Class<?> bare = lookup(message + suffix);
        if (suffixed != null && bare != null) {
            throw new OracleException(
                    message + "Data" + suffix + " and " + message + suffix + " both exist; this jar "
                            + "does not follow the naming rule this oracle resolves by, and choosing "
                            + "between them would be a guess");
        }
        if (suffixed != null) {
            return suffixed;
        }
        if (bare != null) {
            return bare;
        }
        throw new OracleException("neither " + MESSAGE_PACKAGE + "." + message + "Data" + suffix
                + " nor " + MESSAGE_PACKAGE + "." + message + suffix + " exists in the oracle jar");
    }

    private static Class<?> lookup(String simpleName) {
        try {
            return Class.forName(MESSAGE_PACKAGE + "." + simpleName);
        } catch (ClassNotFoundException absent) {
            return null;
        }
    }

    private static Message newData(String message) throws Exception {
        Class<?> dataClass = generated(message, "");
        Object instance = dataClass.getDeclaredConstructor().newInstance();
        if (!(instance instanceof Message data)) {
            throw new OracleException(dataClass.getSimpleName() + " is not a Kafka Message");
        }
        return data;
    }

    private static Message readConverter(String message, JsonNode value, short version) throws Exception {
        Class<?> converter = generated(message, "JsonConverter");
        Method read = converter.getMethod("read", JsonNode.class, short.class);
        Object instance = read.invoke(null, value, version);
        if (!(instance instanceof Message data)) {
            throw new OracleException(converter.getSimpleName() + ".read did not return a Message");
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
