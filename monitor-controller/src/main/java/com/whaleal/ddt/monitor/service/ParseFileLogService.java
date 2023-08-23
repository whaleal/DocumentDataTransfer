package com.whaleal.ddt.monitor.service;

/**
 * 文件日志解析服务接口
 *
 * This service interface defines methods related to parsing file logs.
 *
 * Author: liheping
 */
public interface ParseFileLogService {

    /**
     * 读取文件日志
     *
     * Reads file logs from the specified file path starting at the given position.
     *
     * @param filePath  文件路径
     * @param position  起始位置
     */
    long readFile(String filePath, long position);
}
