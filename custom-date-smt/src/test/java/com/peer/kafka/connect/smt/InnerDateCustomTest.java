package com.peer.kafka.connect.smt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class InnerDateCustomTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final long DATE_PLUS_TIME_UNIX_MICROS;
    private static final long DATE_PLUS_TIME_UNIX_NANOS;
    private static final long DATE_PLUS_TIME_UNIX_SECONDS;
    private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
    private static final String DATE_PLUS_TIME_STRING;

    private final InnerDateCustom<SourceRecord> xformKey = new InnerDateCustom.Key<>();
    private final InnerDateCustom<SourceRecord> xformValue = new InnerDateCustom.Value<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);
        // 86 401 234 milliseconds
        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
        // 86 401 234 123 microseconds
        DATE_PLUS_TIME_UNIX_MICROS = DATE_PLUS_TIME_UNIX * 1000 + 123;
        // 86 401 234 123 456 nanoseconds
        DATE_PLUS_TIME_UNIX_NANOS = DATE_PLUS_TIME_UNIX_MICROS * 1000 + 456;
        // 86401 seconds
        DATE_PLUS_TIME_UNIX_SECONDS = DATE_PLUS_TIME.getTimeInMillis() / 1000;
        DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
    }


    // Configuration

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testConfigNoTargetType() {
        assertThrows(ConfigException.class, () -> xformValue.configure(Collections.<String, String>emptyMap()));
    }

    @Test
    public void testConfigInvalidTargetType() {
        assertThrows(ConfigException.class,
                () -> xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "invalid")));
    }

    @Test
    public void testConfigInvalidUnixPrecision() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "unix");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "invalid");
        assertThrows(ConfigException.class, () -> xformValue.configure(config));
    }

    @Test
    public void testConfigValidUnixPrecision() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "unix");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "seconds");
        assertDoesNotThrow(() -> xformValue.configure(config));
    }

    @Test
    public void testConfigMissingFormat() {
        assertThrows(ConfigException.class,
                () -> xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "string")));
    }

    @Test
    public void testConfigInvalidFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "string");
        config.put(InnerDateCustom.FORMAT_CONFIG, "bad-format");
        assertThrows(ConfigException.class, () -> xformValue.configure(config));
    }

    // Conversions without schemas (most flexible Timestamp -> other types)

    @Test
    public void testSchemalessIdentity() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToDate() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToTime() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToUnix() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testSchemalessTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "string");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }


    // Conversions without schemas (core types -> most flexible Timestamp format)

    @Test
    public void testSchemalessDateToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimeToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(TIME.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessUnixToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_UNIX));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }


    // Conversions with schemas (most flexible Timestamp -> other types)

    @Test
    public void testWithSchemaIdentity() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToDate() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Date.SCHEMA, transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToTime() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Time.SCHEMA, transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToUnix() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.INT64_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "string");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }

    // Null-value conversions schemaless

    @Test
    public void testSchemalessNullValueToString() {
        testSchemalessNullValueConversion("string");
        testSchemalessNullFieldConversion("string");
    }
    @Test
    public void testSchemalessNullValueToDate() {
        testSchemalessNullValueConversion("Date");
        testSchemalessNullFieldConversion("Date");
    }
    @Test
    public void testSchemalessNullValueToTimestamp() {
        testSchemalessNullValueConversion("Timestamp");
        testSchemalessNullFieldConversion("Timestamp");
    }
    @Test
    public void testSchemalessNullValueToUnix() {
        testSchemalessNullValueConversion("unix");
        testSchemalessNullFieldConversion("unix");
    }

    @Test
    public void testSchemalessNullValueToTime() {
        testSchemalessNullValueConversion("Time");
        testSchemalessNullFieldConversion("Time");
    }

    private void testSchemalessNullValueConversion(String targetType) {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, targetType);
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(null));

        assertNull(transformed.valueSchema());
        assertNull(transformed.value());
    }

    private void testSchemalessNullFieldConversion(String targetType) {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, targetType);
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(null));

        assertNull(transformed.valueSchema());
        assertNull(transformed.value());
    }

    // Conversions with schemas (core types -> most flexible Timestamp format)

    @Test
    public void testWithSchemaDateToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Date.SCHEMA, DATE.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimeToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Time.SCHEMA, TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaUnixToTimestamp() {
        xformValue.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Schema.INT64_SCHEMA, DATE_PLUS_TIME_UNIX));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Schema.STRING_SCHEMA, DATE_PLUS_TIME_STRING));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    // Null-value conversions with schema

    @Test
    public void testWithSchemaNullValueToTimestamp() {
        testWithSchemaNullValueConversion("Timestamp", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToTimestamp() {
        testWithSchemaNullFieldConversion("Timestamp", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToUnix() {
        testWithSchemaNullValueConversion("unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", InnerDateCustom.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", InnerDateCustom.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToUnix() {
        testWithSchemaNullFieldConversion("unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", InnerDateCustom.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", InnerDateCustom.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToTime() {
        testWithSchemaNullValueConversion("Time", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToTime() {
        testWithSchemaNullFieldConversion("Time", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_TIME_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToDate() {
        testWithSchemaNullValueConversion("Date", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToDate() {
        testWithSchemaNullFieldConversion("Date", Schema.OPTIONAL_INT64_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", InnerDateCustom.OPTIONAL_TIME_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", InnerDateCustom.OPTIONAL_DATE_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", Schema.OPTIONAL_STRING_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, InnerDateCustom.OPTIONAL_DATE_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToString() {
        testWithSchemaNullValueConversion("string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", InnerDateCustom.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", InnerDateCustom.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToString() {
        testWithSchemaNullFieldConversion("string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", InnerDateCustom.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", InnerDateCustom.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", InnerDateCustom.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    }

    private void testWithSchemaNullValueConversion(String targetType, Schema originalSchema, Schema expectedSchema) {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, targetType);
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(originalSchema, null));

        assertEquals(expectedSchema, transformed.valueSchema());
        assertNull(transformed.value());
    }

    private void testWithSchemaNullFieldConversion(String targetType, Schema originalSchema, Schema expectedSchema) {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, targetType);
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        xformValue.configure(config);
        SchemaBuilder structSchema = SchemaBuilder.struct()
                .field("ts", originalSchema)
                .field("other", Schema.STRING_SCHEMA);

        SchemaBuilder expectedStructSchema = SchemaBuilder.struct()
                .field("ts", expectedSchema)
                .field("other", Schema.STRING_SCHEMA);

        Struct original = new Struct(structSchema);
        original.put("ts", null);
        original.put("other", "test");

        // Struct field is null
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structSchema.build(), original));

        assertEquals(expectedStructSchema.build(), transformed.valueSchema());
        assertNull(requireStruct(transformed.value(), "").get("ts"));

        // entire Struct is null
        transformed = xformValue.apply(createRecordWithSchema(structSchema.optional().build(), null));

        assertEquals(expectedStructSchema.optional().build(), transformed.valueSchema());
        assertNull(transformed.value());
    }

    // Convert field instead of entire key/value

    @Test
    public void testSchemalessFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Date");
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        Object value = Collections.singletonMap("ts", DATE_PLUS_TIME.getTime());
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        assertNull(transformed.valueSchema());
        assertEquals(Collections.singletonMap("ts", DATE.getTime()), transformed.value());
    }

    @Test
    public void testWithSchemaFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        // ts field is a unix timestamp
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();

        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX);
        original.put("other", "test");

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
        assertEquals("test", ((Struct) transformed.value()).get("other"));
    }

    @Test
    public void testWithSchemaFieldConversion_Micros() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "microseconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with microseconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_MICROS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testWithSchemaFieldConversion_Nanos() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "nanoseconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with microseconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_NANOS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testWithSchemaFieldConversion_Seconds() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "ts");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "seconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with seconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Calendar expectedDate = GregorianCalendar.getInstance(UTC);
        expectedDate.setTimeInMillis(0L);
        expectedDate.add(Calendar.DATE, 1);
        expectedDate.add(Calendar.SECOND, 1);

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(expectedDate.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testSchemalessStringToUnix_Micros() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "unix");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "microseconds");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        // Conversion loss as expected, sub-millisecond part is not stored in pivot java.util.Date
        assertEquals(86401234000L, transformed.value());
    }

    @Test
    public void testSchemalessStringToUnix_Nanos() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "unix");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "nanoseconds");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        // Conversion loss as expected, sub-millisecond part is not stored in pivot java.util.Date
        assertEquals(86401234000000L, transformed.value());
    }

    @Test
    public void testSchemalessStringToUnix_Seconds() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "unix");
        config.put(InnerDateCustom.UNIX_PRECISION_CONFIG, "seconds");
        config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX_SECONDS, transformed.value());
    }

    // Validate Key implementation in addition to Value

    @Test
    public void testKey() {
        xformKey.configure(Collections.singletonMap(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime(), null, null));

        assertNull(transformed.keySchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.key());
    }

    private SourceRecord createRecordWithSchema(Schema schema, Object value) {
        return new SourceRecord(null, null, "topic", 0, schema, value);
    }

    private SourceRecord createRecordSchemaless(Object value) {
        return createRecordWithSchema(null, value);
    }


    @Test
    public void testSchemalessFieldConversion_1() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "info.ts");
        //config.put(InnerDateCustom.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);

        Map<String, Object> data = new HashMap<>();

        JSONObject innerObj = new JSONObject();
        innerObj.put("name", "John");
        innerObj.put("age", 30);
        innerObj.put("city", "New York");
        innerObj.put("ts", 1679420861996L);

        data.put("id", 123);
        data.put("ts", 1679420861996L);
        data.put("info", innerObj);

        //System.out.println(data);


        Object value = data;
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        assertNull(transformed.valueSchema());
        java.util.Date actual = JsonPath.read(transformed.value(), config.get(InnerDateCustom.FIELD_CONFIG));
        assertEquals(new java.util.Date(1679420861996L), actual);
    }

    @Test
    public void testWithSchemaFieldConversion1() {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "users[*].age");
        xformValue.configure(config);

        Map<String, Object> json = new HashMap<String, Object>();
        JSONArray array = new JSONArray();
        JSONObject obj1 = new JSONObject();
        obj1.put("name", "John");
        obj1.put("age", 1679420861996L);
        JSONObject obj2 = new JSONObject();
        obj2.put("name", "Jane");
        //obj2.put("age", 1679420861996L);

        JSONObject obj3 = new JSONObject();
        obj2.put("name", "Jane");
        obj2.put("age", 1679420861996L);

        array.add(obj1);
        array.add(obj2);
        array.add(obj3);
        json.put("users", array);

        Object value = json;
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        assertNull(transformed.valueSchema());

        JSONArray actual = JsonPath.read(transformed.value(), config.get(InnerDateCustom.FIELD_CONFIG));

        java.util.Date date= (java.util.Date) actual.get(0);
        assertEquals(new java.util.Date(1679420861996L), date);

        java.util.Date date1= (java.util.Date) actual.get(1);
        assertEquals(new java.util.Date(1679420861996L), date1);
    }

    @Test
    public void testWithSubArrayObjDateFieldConversion() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "hello[*].date");
        xformValue.configure(config);


        String jsonStr = "{\"ordertime\":1497014222380,\"orderid\":18,\"itemid\":\"Item_184\",\"address\":{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391},\"hello\":[{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391},{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391},{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":null}]}";

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {});


        Object value = jsonMap;
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        JSONArray actual = JsonPath.read(transformed.value(), config.get(InnerDateCustom.FIELD_CONFIG));

        java.util.Date date= (java.util.Date) actual.get(0);
        assertEquals(new java.util.Date(1679710746391L), date);

        java.util.Date date1= (java.util.Date) actual.get(1);
        assertEquals(new java.util.Date(1679710746391L), date1);

        //assertEquals(null, actual.);

    }

    @Test
    public void testWithSubArrayObjDateFieldConversionWithWrongPath() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(InnerDateCustom.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(InnerDateCustom.FIELD_CONFIG, "hello1[*].date");
        xformValue.configure(config);


        String jsonStr = "{\"ordertime\":1497014222380,\"orderid\":18,\"itemid\":\"Item_184\",\"address\":{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391},\"hello\":[{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391},{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":null},{\"city\":\"Mountain View\",\"state\":\"CA\",\"zipcode\":94041,\"date\":1679710746391}]}";

        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap = objectMapper.readValue(jsonStr, new TypeReference<Map<String, Object>>() {});


        Object value = jsonMap;
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        JSONArray actual = JsonPath.read(transformed.value(), "hello[*].date");

        long date= (long) actual.get(0);
        assertEquals(1679710746391L, date);

    }
}