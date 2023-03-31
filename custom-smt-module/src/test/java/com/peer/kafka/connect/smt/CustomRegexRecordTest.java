package com.peer.kafka.connect.smt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CustomRegexRecordTest {
    public CustomRegexRecordTest() {
    }

    private static SinkRecord apply(String regex, String replacement, String field, Object record) {
        Map<String, String> props = new HashMap();
        props.put("regex", regex);
        props.put("replacement", replacement);
        props.put("field", field);
        CustomRegexRecord<SinkRecord> router = new CustomRegexRecord<>();
        router.configure(props);
        SinkRecord sinkRecord = router.apply(new SinkRecord(null, 0, (Schema) null, (Object) null, (Schema) null, (Object) record, 0L));
        router.close();
        return sinkRecord;
    }

    @Test
    public void slice() throws IOException {

        String regex = "^\\\"(.*)\\\"$";
        String replacement = "$1";
        String field = "_etag";

        String inputRecord = "{\"name\":\"hello\",\"_etag\":\"\\\"b20044f7-0000-0100-0000-612f7aa30000\\\"\",\"age\":20}";

        String expectedValue = "{\"name\":\"hello\",\"_etag\":\"b20044f7-0000-0100-0000-612f7aa30000\",\"age\":20}";

        ObjectMapper objectMapper = new ObjectMapper();

        Object record = objectMapper.<Map<String, Object>>readValue(inputRecord, new TypeReference<Map<String, Object>>() {
        });

        Object expected = objectMapper.<Map<String, Object>>readValue(expectedValue, new TypeReference<Map<String, Object>>() {
        });

        SinkRecord sinkRecord = apply(regex, replacement, field, record);
        Assertions.assertEquals(expected, sinkRecord.value());
    }
}

