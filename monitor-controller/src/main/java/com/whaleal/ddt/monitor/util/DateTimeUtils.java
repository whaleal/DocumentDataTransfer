package com.whaleal.ddt.monitor.util;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * @author cc
 */
public class DateTimeUtils {

    public static int offset = 0;

    static {
        TimeZone aDefault = TimeZone.getDefault();
        // 默认时区
        offset = aDefault.getRawOffset() / 1000;
    }

    public static Long stringToStamp(String str, String format) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(format);
        LocalDateTime parse = LocalDateTime.parse(str, dateTimeFormatter);
        return parse.toInstant(ZoneOffset.ofTotalSeconds(offset)).toEpochMilli();
    }

    public static Long stringToStamp(String str) {
       return stringToStamp(str,"yyyy-MM-dd HH:mm:ss.SSS");
    }

    public static void main(String[] args) {
        System.out.println(stringToStamp("2023-02-09 13:43:34.362", "yyyy-MM-dd HH:mm:ss.SSS"));
    }

}
