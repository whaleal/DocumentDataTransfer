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
package com.whaleal.ddt.cache;

import com.mongodb.client.model.WriteModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lhp
 * @time 2021-05-31 13:12:12
 * @desc 批量数据实体类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Log4j2
public class BatchDataEntity implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * 批次号
     */
    private long batchNo;
    /**
     * 操作行为
     */
    private String operation;
    /**
     * 库表名
     */
    private String ns;
    /**
     * 数据来源
     */
    private String sourceDsName;
    /**
     * 数据目的地
     */
    private String targetDsName;
    /**
     * work名称
     */
    private String workName;
    /**
     * 数据集合
     */
    private List<WriteModel<Document>> dataList = new ArrayList();

    public BatchDataEntity(String workName, String targetDsName, int initialCapacity) {
        this.workName = workName;
        this.targetDsName = targetDsName;
        this.dataList = new ArrayList(initialCapacity);
    }
}
