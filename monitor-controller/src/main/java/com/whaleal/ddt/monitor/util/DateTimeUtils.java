/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present ] [Whaleal]
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: [www.whaleal.com]
 */
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
