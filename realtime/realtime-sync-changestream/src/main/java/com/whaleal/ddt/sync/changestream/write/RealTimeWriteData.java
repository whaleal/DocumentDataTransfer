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
package com.whaleal.ddt.sync.changestream.write;


import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.whaleal.ddt.realtime.common.write.BaseRealTimeWriteData;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;


/**
 * 实时写入数据的类。
 *
 * @author liheping
 */
@Log4j2
public class RealTimeWriteData extends BaseRealTimeWriteData<ChangeStreamDocument<Document>> {
    public RealTimeWriteData(String workName, String dsName, int bucketSize) {
        super(workName, dsName, bucketSize);
    }
}
