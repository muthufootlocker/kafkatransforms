package com.peer.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.kafka.connect.errors.DataException;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {

    static String json = "{\n" +
            "    \"id\": \"52972dfe-940b-4def-aac2-30b9e9f7b6df\",\n" +
            "    \"organizationId\": \"1d35cadb-5370-4c92-bd2a-4128b2d81e07\",\n" +
            "    \"code\": \"ncp7rt\",\n" +
            "    \"iat\": \"2021-09-01T13:05:33.2958929\",\n" +
            "    \"exp\": \"2021-09-02T13:05:33.2958929\",\n" +
            "    \"delegationToken\": {\n" +
            "        \"id\": \"c783ddba-90a0-4e6e-9c39-c10a676f37ac\",\n" +
            "        \"organizationId\": \"1d35cadb-5370-4c92-bd2a-4128b2d81e07\",\n" +
            "        \"token\": \"brTaTL\",\n" +
            "        \"iat\": \"2021-09-01T13:05:39.5175631\",\n" +
            "        \"exp\": \"2021-09-02T13:05:39.5175631\",\n" +
            "        \"used\": false\n" +
            "    },\n" +
            "    \"used\": true,\n" +
            "    \"_rid\": \"OBojALgfSxVCAAAAAAAAAA==\",\n" +
            "    \"_self\": \"dbs/OBojAA==/colls/OBojALgfSxU=/docs/OBojALgfSxVCAAAAAAAAAA==/\",\n" +
            "    \"_etag\": \"\\\"b20044f7-0000-0100-0000-612f7aa30000\\\"\",\n" +
            "    \"_attachments\": \"attachments/\",\n" +
            "    \"_ts\": 1630501539\n" +
            "}";



    static String regex = "\"([^\"]+)\"";
    static String replacement ="$1";
    static String field ="_etag";
    public static void main(String[] args) {

        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, String> map = mapper.readValue(json, Map.class);
        } catch (Exception ex) {

        }

        final Map<String, Object> value = requireMap(json);

        final HashMap<String, Object> updatedValue = new HashMap<>(value);

        convertValue(updatedValue, field);

        System.out.println(updatedValue);
    }


    public static Map<String, Object> requireMap(Object value) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported in absence of schema for regexRecordValue");
        } else {
            return (Map) value;
        }
    }

    private static void convertValue(Map<String, Object> json, String jsonPath) {
        Pattern pattern = Pattern.compile(regex);
        Configuration conf = Configuration.defaultConfiguration();
        conf = conf.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        DocumentContext jsonContext = JsonPath.using(conf).parse(json);
        Object currentValue = jsonContext.read(jsonPath);
        try {
            // Update the value at the specified path
            if (currentValue != null) {
                jsonContext.set(jsonPath, applyRegex(currentValue, pattern, replacement, field));
            }
        } catch (Exception e) {
            // The current value cannot be parsed as a date
        }
    }

    private static Object applyRegex(Object jsonString, Pattern pattern, String replacement, String path) {
        Configuration conf = Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
        DocumentContext jsonContext = JsonPath.using(conf).parse(jsonString);
        Object currentValue = jsonContext.read(path);

        if (currentValue != null) {
            Matcher matcher = pattern.matcher(currentValue.toString());
            if (matcher.find()) {
                currentValue = matcher.replaceFirst(replacement);
            }
        }
        return currentValue;
    }
}
