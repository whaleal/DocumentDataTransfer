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
    long readFile(String filePath, long position, long endPosition);

    void readGZFile(String filePath);
}
