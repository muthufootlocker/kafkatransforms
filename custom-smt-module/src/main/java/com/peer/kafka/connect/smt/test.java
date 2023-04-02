package com.peer.kafka.connect.smt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.*;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import org.apache.kafka.connect.errors.DataException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String z[]) throws ParseException {

        String dateString = "2023-02-07T15:39:53.0622557";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS");
        //dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse(dateString);
        System.out.println(date);

       // dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        //String isoDate = dateFormat.format(date);
        //System.out.println(isoDate);
    }
}

