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


import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author cc
 */
public class R extends HashMap<String, Object> {
    /**
     * 序列化编号
     */
    private static final long serialVersionUID = 1L;

    /**
     * 实例化R
     * 默认状态为正常
     */
    public R() {
        put("code", BizCodeEnum.SUCCESS_CODE.getCode());
    }

    /**
     * 实例化R
     * 默认状态为错误
     *
     * @return the r
     */
    public static R error() {
        return error(BizCodeEnum.ERROR_SYSTEM.getCode(), BizCodeEnum.ERROR_SYSTEM.getMsg());
    }


    /**
     * 实例化R
     *
     * @param code 状态码
     * @param msg  消息
     * @return the r
     */
    public static R error(int code, String msg) {
        R r = new R();
        r.put("code", code);
        r.put("msg", msg);
        return r;
    }


    /**
     * 实例化R
     *
     * @param bizCodeEnum bizCodeEnum
     * @return the r
     */
    public static R noExpect(BizCodeEnum bizCodeEnum) {
        R r = new R();
        r.put("code", bizCodeEnum.getCode());
        r.put("msg", bizCodeEnum.getMsg());
        return r;
    }


    /**
     * 实例化R
     *
     * @param map the map
     * @return the r
     */
    public static R ok(Map<String, Object> map) {
        R r = new R();
        r.putAll(map);
        return r;
    }

    /**
     * 实例化R
     *
     * @return the r
     */
    public static R ok() {
        return new R();
    }

    @Override
    public R put(String key, Object value) {
        super.put(key, value);
        return this;
    }

    /**
     * 获取状态码
     *
     * @return the code
     */
    public int getCode() {
        return (Integer) this.get("code");
    }

    /**
     * 获取data
     *
     * @param key    获取指定key的名字
     * @param tClass 类名
     * @return the data
     */
    public <T> T getData(String key, Class<T> tClass) {
        // get("data") 默认是map类型 所以再由map转成string再转json
        Object data = get(key);
        return JSON.parseObject(JSON.toJSONString(data), tClass);
    }

    /**
     * 获取List data
     *
     * @param key    获取指定key的名字
     * @param tClass 类名
     * @return the array data
     */
    public <T> List<T> getArrayData(String key, Class<T> tClass) {
        // get("data") 默认是map类型 所以再由map转成string再转json
        Object data = get(key);
        return JSON.parseArray(JSON.toJSONString(data), tClass);
    }
}
