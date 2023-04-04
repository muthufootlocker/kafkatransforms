package com.peer.kafka.connect.smt;

import com.jayway.jsonpath.*;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class CustomInnerTimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Convert timestamps between different formats such as Unix epoch, strings, and Connect Date/Timestamp types."
                    + "Applies to individual fields or to the entire value."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + CustomInnerTimestampConverter.Key.class.getName() + "</code>) "
                    + "or value (<code>" + CustomInnerTimestampConverter.Value.class.getName() + "</code>).";

    public static final String FIELD_CONFIG = "field";
    public static final String TARGET_TYPE_CONFIG = "target.type";
    public static final String FORMAT_CONFIG = "format";
    public static final String UNIX_PRECISION_CONFIG = "unix.precision";
    public static final Schema OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().schema();
    public static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().schema();
    public static final Schema OPTIONAL_TIME_SCHEMA = Time.builder().optional().schema();
    private static final String FIELD_DEFAULT = "";
    private static final String FORMAT_DEFAULT = "";
    private static final String UNIX_PRECISION_DEFAULT = "milliseconds";
    private static final String PURPOSE = "converting timestamp formats";
    private static final String TYPE_STRING = "string";
    private static final String TYPE_UNIX = "unix";
    private static final String TYPE_DATE = "Date";
    private static final String TYPE_TIME = "Time";
    private static final String TYPE_TIMESTAMP = "Timestamp";
    private static final String UNIX_PRECISION_MILLIS = "milliseconds";
    private static final String UNIX_PRECISION_MICROS = "microseconds";
    private static final String UNIX_PRECISION_NANOS = "nanoseconds";
    private static final String UNIX_PRECISION_SECONDS = "seconds";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.HIGH,
                    "The field containing the timestamp, or empty if the entire value is a timestamp")
            .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.ValidString.in(TYPE_STRING, TYPE_UNIX, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP),
                    ConfigDef.Importance.HIGH,
                    "The desired timestamp representation: string, unix, Date, Time, or Timestamp")
            .define(FORMAT_CONFIG, ConfigDef.Type.STRING, FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM,
                    "A SimpleDateFormat-compatible format for the timestamp. Used to generate the output when type=string "
                            + "or used to parse the input if the input is a string.")
            .define(UNIX_PRECISION_CONFIG, ConfigDef.Type.STRING, UNIX_PRECISION_DEFAULT,
                    ConfigDef.ValidString.in(
                            UNIX_PRECISION_NANOS, UNIX_PRECISION_MICROS,
                            UNIX_PRECISION_MILLIS, UNIX_PRECISION_SECONDS),
                    ConfigDef.Importance.LOW,
                    "The desired Unix precision for the timestamp: seconds, milliseconds, microseconds, or nanoseconds. " +
                            "Used to generate the output when type=unix or used to parse the input if the input is a Long." +
                            "Note: This SMT will cause precision loss during conversions from, and to, values with sub-millisecond components.");
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Map<String, CustomInnerTimestampConverter.TimestampTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(TYPE_STRING, new CustomInnerTimestampConverter.TimestampTranslator() {
            @Override
            public Date toRaw(CustomInnerTimestampConverter.Config config, Object orig) {
                if (!(orig instanceof String))
                    throw new DataException("Expected string timestamp to be a String, but found " + orig.getClass());
                try {
                    return config.format.parse((String) orig);
                } catch (ParseException e) {
                    throw new DataException("Could not parse timestamp: value (" + orig + ") does not match pattern ("
                            + config.format.toPattern() + ")", e);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
            }

            @Override
            public String toType(CustomInnerTimestampConverter.Config config, Date orig) {
                synchronized (config.format) {
                    return config.format.format(orig);
                }
            }
        });

        TRANSLATORS.put(TYPE_UNIX, new CustomInnerTimestampConverter.TimestampTranslator() {
            @Override
            public Date toRaw(CustomInnerTimestampConverter.Config config, Object orig) {
                if (!(orig instanceof Long))
                    throw new DataException("Expected Unix timestamp to be a Long, but found " + orig.getClass());
                Long unixTime = (Long) orig;
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.SECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MICROS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.MICROSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_NANOS:
                        return Timestamp.toLogical(Timestamp.SCHEMA, TimeUnit.NANOSECONDS.toMillis(unixTime));
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return Timestamp.toLogical(Timestamp.SCHEMA, unixTime);
                }
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA;
            }

            @Override
            public Long toType(CustomInnerTimestampConverter.Config config, Date orig) {
                Long unixTimeMillis = Timestamp.fromLogical(Timestamp.SCHEMA, orig);
                switch (config.unixPrecision) {
                    case UNIX_PRECISION_SECONDS:
                        return TimeUnit.MILLISECONDS.toSeconds(unixTimeMillis);
                    case UNIX_PRECISION_MICROS:
                        return TimeUnit.MILLISECONDS.toMicros(unixTimeMillis);
                    case UNIX_PRECISION_NANOS:
                        return TimeUnit.MILLISECONDS.toNanos(unixTimeMillis);
                    case UNIX_PRECISION_MILLIS:
                    default:
                        return unixTimeMillis;
                }
            }
        });

        TRANSLATORS.put(TYPE_DATE, new CustomInnerTimestampConverter.TimestampTranslator() {
            @Override
            public Date toRaw(CustomInnerTimestampConverter.Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Date to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Dates are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_DATE_SCHEMA : org.apache.kafka.connect.data.Date.SCHEMA;
            }

            @Override
            public Date toType(CustomInnerTimestampConverter.Config config, Date orig) {
                Calendar result = Calendar.getInstance(UTC);
                result.setTime(orig);
                result.set(Calendar.HOUR_OF_DAY, 0);
                result.set(Calendar.MINUTE, 0);
                result.set(Calendar.SECOND, 0);
                result.set(Calendar.MILLISECOND, 0);
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIME, new CustomInnerTimestampConverter.TimestampTranslator() {
            @Override
            public Date toRaw(CustomInnerTimestampConverter.Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Time to be a java.util.Date, but found " + orig.getClass());
                // Already represented as a java.util.Date and Connect Times are a subset of valid java.util.Date values
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIME_SCHEMA : Time.SCHEMA;
            }

            @Override
            public Date toType(CustomInnerTimestampConverter.Config config, Date orig) {
                Calendar origCalendar = Calendar.getInstance(UTC);
                origCalendar.setTime(orig);
                Calendar result = Calendar.getInstance(UTC);
                result.setTimeInMillis(0L);
                result.set(Calendar.HOUR_OF_DAY, origCalendar.get(Calendar.HOUR_OF_DAY));
                result.set(Calendar.MINUTE, origCalendar.get(Calendar.MINUTE));
                result.set(Calendar.SECOND, origCalendar.get(Calendar.SECOND));
                result.set(Calendar.MILLISECOND, origCalendar.get(Calendar.MILLISECOND));
                return result.getTime();
            }
        });

        TRANSLATORS.put(TYPE_TIMESTAMP, new CustomInnerTimestampConverter.TimestampTranslator() {
            @Override
            public Date toRaw(CustomInnerTimestampConverter.Config config, Object orig) {
                if (!(orig instanceof Date))
                    throw new DataException("Expected Timestamp to be a java.util.Date, but found " + orig.getClass());
                return (Date) orig;
            }

            @Override
            public Schema typeSchema(boolean isOptional) {
                return isOptional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA;
            }

            @Override
            public Date toType(CustomInnerTimestampConverter.Config config, Date orig) {
                return orig;
            }
        });
    }

    private CustomInnerTimestampConverter.Config config;
    private Cache<Schema, Schema> schemaUpdateCache;

    public static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        final String field = simpleConfig.getString(FIELD_CONFIG);
        final String type = simpleConfig.getString(TARGET_TYPE_CONFIG);
        String formatPattern = simpleConfig.getString(FORMAT_CONFIG);
        final String unixPrecision = simpleConfig.getString(UNIX_PRECISION_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

        if (type.equals(TYPE_STRING) && isBlank(formatPattern)) {
            throw new ConfigException("InnerDateTransform requires format option to be specified when using string timestamps");
        }
        SimpleDateFormat format = null;
        if (!isBlank(formatPattern)) {
            try {
                format = new SimpleDateFormat(formatPattern);
                format.setTimeZone(UTC);
            } catch (IllegalArgumentException e) {
                throw new ConfigException("InnerDateTransform requires a SimpleDateFormat-compatible pattern for string timestamps: "
                        + formatPattern, e);
            }
        }
        config = new CustomInnerTimestampConverter.Config(field, type, format, unixPrecision);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private R applyWithSchema(R record) {
        final Schema schema = operatingSchema(record);
        if (config.field.isEmpty()) {
            Object value = operatingValue(record);
            // New schema is determined by the requested target timestamp type
            Schema updatedSchema = TRANSLATORS.get(config.type).typeSchema(schema.isOptional());
            return newRecord(record, updatedSchema, convertTimestamp(value, timestampTypeFromSchema(schema)));
        } else {
            final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);
            Schema updatedSchema = schemaUpdateCache.get(schema);
            if (updatedSchema == null) {
                SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
                for (Field field : schema.fields()) {
                    if (field.name().equals(config.field)) {
                        builder.field(field.name(), TRANSLATORS.get(config.type).typeSchema(field.schema().isOptional()));
                    } else {
                        builder.field(field.name(), field.schema());
                    }
                }
                if (schema.isOptional())
                    builder.optional();
                if (schema.defaultValue() != null) {
                    Struct updatedDefaultValue = applyValueWithSchema((Struct) schema.defaultValue(), builder);
                    builder.defaultValue(updatedDefaultValue);
                }

                updatedSchema = builder.build();
                schemaUpdateCache.put(schema, updatedSchema);
            }

            Struct updatedValue = applyValueWithSchema(value, updatedSchema);
            return newRecord(record, updatedSchema, updatedValue);
        }
    }

    private Struct applyValueWithSchema(Struct value, Schema updatedSchema) {
        if (value == null) {
            return null;
        }
        Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object updatedFieldValue;
            if (field.name().equals(config.field)) {
                updatedFieldValue = convertTimestamp(value.get(field), timestampTypeFromSchema(field.schema()));
            } else {
                updatedFieldValue = value.get(field);
            }
            updatedValue.put(field.name(), updatedFieldValue);
        }
        return updatedValue;
    }

    private R applySchemaless(R record) {
        Object rawValue = operatingValue(record);
        if (rawValue == null || config.field.isEmpty()) {
            return newRecord(record, null, convertTimestamp(rawValue));
        } else {
            final Map<String, Object> value = requireMap(rawValue, PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            if (config.field.contains("[")) {
                convertArrayOfObject(updatedValue, config.field);
            } else if (config.field.contains(".")) {
                convertSubDocDate(updatedValue, config.field);
            } else {
                updatedValue.put(config.field, convertTimestamp(value.get(config.field)));
            }
            return newRecord(record, null, updatedValue);
        }
    }

    private void convertSubDocDate(Map<String, Object> json, String jsonPath){

        try {
        Configuration conf = Configuration.defaultConfiguration();
        conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        DocumentContext jsonContext = JsonPath.using(conf).parse(json);
        Object currentValue = jsonContext.read(jsonPath);
            // Update the value at the specified path
            if(currentValue!=null) {
                jsonContext.set(jsonPath, convertTimestamp(currentValue));
            }
        } catch (Exception e) {
            // The current value cannot be parsed as a date
        }
    }

    private void convertArrayOfObject(Map<String, Object> json, String jsonPath) {
        try {
            Configuration conf = Configuration.defaultConfiguration();
            conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);

            List<Object> valuesToUpdate = JsonPath.using(conf).parse(json).read(jsonPath);
            int totalValuesToUpdate = valuesToUpdate.size();
            if (totalValuesToUpdate > 0) {
                DocumentContext ctx = JsonPath.using(conf).parse(json);
                String pathToUpdatePrefix = jsonPath.substring(0, jsonPath.indexOf("[*]"));
                String pathToUpdatePostfix = jsonPath.substring(jsonPath.indexOf("."), jsonPath.length());
                for (int i = 0; i < totalValuesToUpdate; i++) {
                    String pathToUpdate = pathToUpdatePrefix + "[" + i + "]" + pathToUpdatePostfix;
                    if (ctx.read(pathToUpdate) != null) {
                        ctx.set(pathToUpdate, convertTimestamp(valuesToUpdate.get(i)));
                    }
                }
            }
        } catch (Exception e) {
            //throw new ConnectException("Path not found while converting the array of object in CustomInnerTimestampConverter.class");
        }
    }

    private void convertDate(HashMap<String, Object> updatedValue, String toString) {
        String path = toString; // the path to the value that needs to be updated
        Object currentValue = updatedValue; // initialize currentValue to the entire map

// traverse the map based on the path and retrieve the current value
        for (String key : path.split("\\.")) {
            if (currentValue instanceof Map) {
                currentValue = ((Map<?, ?>) currentValue).get(key);
            } else {
                //throw new IllegalArgumentException("Invalid path: " + path);
            }
        }

// check if the current value is a string that can be parsed as a date
        try {
            Object date = convertTimestamp(currentValue);
            // update the value in the map at the given path
            Object parent = updatedValue;
            String[] keys = path.split("\\.");
            for (int i = 0; i < keys.length - 1; i++) {
                String key = keys[i];
                Object child = ((Map<?, ?>) parent).get(key);
                if (child == null) {
                    child = new HashMap<>();
                    ((Map<String, Object>) parent).put(key, child);
                }
                parent = child;
            }
            ((Map<String, Object>) parent).put(keys[keys.length - 1], date);
        } catch (Exception e) {
            // the current value cannot be parsed as a date
        }
    }


    /**
     * Determine the type/format of the timestamp based on the schema
     */
    private String timestampTypeFromSchema(Schema schema) {
        if (Timestamp.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIMESTAMP;
        } else if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_DATE;
        } else if (Time.LOGICAL_NAME.equals(schema.name())) {
            return TYPE_TIME;
        } else if (schema.type().equals(Schema.Type.STRING)) {
            // If not otherwise specified, string == user-specified string format for timestamps
            return TYPE_STRING;
        } else if (schema.type().equals(Schema.Type.INT64)) {
            // If not otherwise specified, long == unix time
            return TYPE_UNIX;
        }
        throw new ConnectException("Schema " + schema + " does not correspond to a known timestamp type format");
    }

    /**
     * Infer the type/format of the timestamp based on the raw Java type
     */
    private String inferTimestampType(Object timestamp) {

        // Note that we can't infer all types, e.g. Date/Time/Timestamp all have the same runtime representation as a
        // java.util.Date
        if (timestamp instanceof Date) {
            return TYPE_TIMESTAMP;
        } else if (timestamp instanceof Long) {
            return TYPE_UNIX;
        } else if (timestamp instanceof String) {
            return TYPE_STRING;
        }
        throw new DataException("InnerDateTransform does not support " + timestamp.getClass() + " objects as timestamps");
    }

    /**
     * Convert the given timestamp to the target timestamp format.
     *
     * @param timestamp       the input timestamp, may be null
     * @param timestampFormat the format of the timestamp, or null if the format should be inferred
     * @return the converted timestamp
     */
    private Object convertTimestamp(Object timestamp, String timestampFormat) {
        if (timestamp == null) {
            return null;
        }
        if (timestampFormat == null) {
            timestampFormat = inferTimestampType(timestamp);
        }

        CustomInnerTimestampConverter.TimestampTranslator sourceTranslator = TRANSLATORS.get(timestampFormat);
        if (sourceTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + timestampFormat);
        }
        Date rawTimestamp = sourceTranslator.toRaw(config, timestamp);

        CustomInnerTimestampConverter.TimestampTranslator targetTranslator = TRANSLATORS.get(config.type);
        if (targetTranslator == null) {
            throw new ConnectException("Unsupported timestamp type: " + config.type);
        }
        return targetTranslator.toType(config, rawTimestamp);
    }

    private Object convertTimestamp(Object timestamp) {
        return convertTimestamp(timestamp, null);
    }

    private interface TimestampTranslator {
        /**
         * Convert from the type-specific format to the universal java.util.Date format
         */
        Date toRaw(CustomInnerTimestampConverter.Config config, Object orig);

        /**
         * Get the schema for this format.
         */
        Schema typeSchema(boolean isOptional);

        /**
         * Convert from the universal java.util.Date format to the type-specific format
         */
        Object toType(CustomInnerTimestampConverter.Config config, Date orig);
    }

    // This is a bit unusual, but allows the transformation config to be passed to static anonymous classes to customize
    // their behavior
    private static class Config {
        String field;
        String type;
        SimpleDateFormat format;
        String unixPrecision;

        Config(String field, String type, SimpleDateFormat format, String unixPrecision) {
            this.field = field;
            this.type = type;
            this.format = format;
            this.unixPrecision = unixPrecision;
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends CustomInnerTimestampConverter<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CustomInnerTimestampConverter<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}

