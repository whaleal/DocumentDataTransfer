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
