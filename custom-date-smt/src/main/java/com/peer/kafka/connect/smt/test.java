package com.peer.kafka.connect.smt;

import com.jayway.jsonpath.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class test {
    public static void main(String z[]) {


        Map<String, Object> json = new HashMap<String, Object>();
        JSONArray array = new JSONArray();

        JSONObject obj1 = new JSONObject();
        obj1.put("name", "John");
        obj1.put("age", null);

        JSONObject obj2 = new JSONObject();
        obj2.put("name", "mark");
        //obj2.put("age", 20);

        JSONObject obj3 = new JSONObject();
        obj3.put("name", "Jane");
        obj3.put("age", 30);


        array.add(obj1);
        array.add(obj2);
        array.add(obj3);
        json.put("users", array);

        String jsonPath = "users[*].age";
        convertArrayOfObject(json, jsonPath);
        System.out.print(json);
    }

    private static void convertArrayOfObject(Map<String, Object> json, String jsonPath) {
        try {
            String inputString = jsonPath;
            String[] parts = inputString.split("\\[");
            String basePath = parts[0];

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
                        if (valuesToUpdate.get(i) != null) {
                            int k = Integer.parseInt(valuesToUpdate.get(i).toString()) + 1;
                            ctx.set(pathToUpdate, k);
                        } else {
                            ctx.set(pathToUpdate, valuesToUpdate.get(i));
                        }
                    }
                }
            }
        } catch (PathNotFoundException e) {
            System.out.println("JSON path not found");
        }
    }

    private static void convertDate(Map<String, Object> updatedValue, String toString) {
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
            Object date = new SimpleDateFormat("yyyy-MM-dd").parse("1679420861996");
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
}
