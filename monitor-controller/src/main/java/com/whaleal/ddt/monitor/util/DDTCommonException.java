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

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;


/**
 * @author cc
 */
@Data
@ToString
@NoArgsConstructor
public class DDTCommonException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private String msg;
    /**
     * 默认状态码 为系统错误
     */
    private int code = BizCodeEnum.ERROR_SYSTEM.getCode();

    /**
     * Instantiates a new Whaleal common exception.
     *
     * @param msg the msg
     */
    public DDTCommonException(String msg) {
        super(msg);
        this.msg = msg;
    }

    /**
     * Instantiates a new Whaleal common exception.
     *
     * @param bizCodeEnum the biz code enum
     */
    public DDTCommonException(BizCodeEnum bizCodeEnum) {
        super(bizCodeEnum.getMsg());
        this.msg = bizCodeEnum.getMsg();
        this.code = bizCodeEnum.getCode();
    }

    /**
     * Instantiates a new Whaleal common exception.
     *
     * @param msg  the msg
     * @param code the code
     */
    public DDTCommonException(String msg, int code) {
        super(msg);
        this.msg = msg;
        this.code = code;
    }
}
