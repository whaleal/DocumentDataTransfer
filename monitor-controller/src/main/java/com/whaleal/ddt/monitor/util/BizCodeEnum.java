package com.whaleal.ddt.monitor.util;


/**
 * @author cc
 */

public enum BizCodeEnum {

    // 9 内部错误
    /**
     * 未知异常.
     */
    UNKNOWN_EXCEPTION(901, "系统未知异常"),
    /**
     * 系统错误
     */
    ERROR_SYSTEM(902, "系统错误"),
    /**
     * 网关限流
     */
    LIMIT_GATEWAY(903, "Gateway current limit, please try again later"),
    /**
     * 执行状态失败.
     */
    ERROR_EXE_COMMAND(903, "执行状态失败"),

    // 10 正常执行

    /**
     * 正常执行.
     */
    SUCCESS_CODE(1000, "正常执行"),

    // 11 用户模块

    /**
     * 任务名不符合要求.
     */
    TASK_NAME_ERR(1101, "任务名错误"),
    /**
     * 进程名不符合要求.
     */
    PROCESS_NAME_ERR(1102, "进程名字错误"),
    /**
     * MongoUrl 错误.
     */
    MONGO_URL_ERR(1103, "MongoUrl 错误"),

    /**
     * 更新失败
     */
    UPDATE_CONFIG_FAIL(1901, "更新失败"),
    /**
     * 添加失败
     */
    ADD_CONFIG_FAIL(1903, "添加失败"),
    /**
     * 删除失败
     */
    DELETE_CONFIG_FAIL(1902, "删除失败");


    private final int code;

    private final String msg;

    BizCodeEnum(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets msg.
     *
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }
}
