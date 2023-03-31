package com.peer.kafka.connect.smt;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.RegexValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomRegexRecord<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Update the record value using the configured regular expression and replacement string.<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. If the pattern matches the input record, <code>java.util.regex.Matcher#replaceAll()</code> is used with the replacement string to obtain the new record.";
    public static final ConfigDef CONFIG_DEF;

    static {
        CONFIG_DEF = (new ConfigDef()).define("regex", Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new RegexValidator(), Importance.HIGH, "Regular expression to use for matching.")
                .define("replacement", Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Replacement string.")
                .define("field", Type.STRING, "", Importance.HIGH,
                        "The field containing the record path");
    }

    private Pattern regex;
    private String replacement;
    private String field;

    public CustomRegexRecord() {
    }

    public static Map<String, Object> requireMap(Object value) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for regexRecordValue");
        } else {
            return (Map) value;
        }
    }

    public void configure(Map<String, ?> props) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.regex = Pattern.compile(config.getString("regex"));
        this.replacement = config.getString("replacement");
        this.field = config.getString("field");
    }

    private Object applyRegex(Object currentValue) {

        if (currentValue != null) {
            Matcher matcher = this.regex.matcher(currentValue.toString());
            if (matcher.find()) {
                currentValue = matcher.replaceAll(this.replacement);
            }
        }
        return currentValue;
    }

    public R apply(R record) {
        Object rawValue = record.value();
        if (rawValue != null) {
            final Map<String, Object> value = requireMap(rawValue);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);
            convertRecordValue(updatedValue, this.field);
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), updatedValue, record.timestamp());
        } else {
            return record;
        }
    }

    private void convertRecordValue(Map<String, Object> json, String jsonPath) {

        Configuration conf = Configuration.defaultConfiguration();
        conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        DocumentContext jsonContext = JsonPath.using(conf).parse(json);
        Object currentValue = jsonContext.read(jsonPath);
        try {
            // Update the value at the specified path
            if (currentValue != null) {
                jsonContext.set(jsonPath, applyRegex(currentValue));
            }
        } catch (Exception e) {
            // The current value cannot be parsed as a date
        }
    }

    public void close() {
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    private interface ConfigName {
        String REGEX = "regex";
        String REPLACEMENT = "replacement";
    }
}

