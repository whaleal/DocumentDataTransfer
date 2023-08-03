
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


package com.whaleal.ddt.metadata;

import com.whaleal.ddt.metadata.source.MongoDBMetadata;
import com.whaleal.ddt.metadata.target.ApplyMongoDBMetadata;
import lombok.extern.log4j.Log4j2;

import java.util.Set;

/**
 * MongoDBClusterManager类用于管理MongoDB集群的操作和配置。
 * 该类封装了各种对集群的操作方法,例如删除目标端已经存在的表、输出用户信息、同步数据库表结构、
 * 同步config.setting表、同步数据库表索引信息、启用库分片、同步数据库表Shard Key信息等功能。
 */
@Log4j2
public class MongoDBClusterManager {
    /**
     * 源数据库名称
     */
    private final String sourceDsName;
    /**
     * 目标数据库名称
     */
    private final String targetDsName;
    /**
     * 工作名称
     */
    private final String workName;
    /**
     * 源数据库元数据
     */
    private final MongoDBMetadata sourceMetadata;
    /**
     * 目标数据库元数据
     */
    private final ApplyMongoDBMetadata applyMongoDBMetadata;

    /**
     * 构造函数
     *
     * @param sourceDsName       源数据库名称
     * @param targetDsName       目标数据库名称
     * @param workName           工作名称
     * @param createIndexNum     创建索引数
     * @param createIndexTimeOut 创建索引超时时间
     */
    public MongoDBClusterManager(String sourceDsName, String targetDsName, String workName, int createIndexNum, long createIndexTimeOut) {
        this.sourceDsName = sourceDsName;
        this.targetDsName = targetDsName;
        this.workName = workName;
        sourceMetadata = new MongoDBMetadata(sourceDsName);
        applyMongoDBMetadata = new ApplyMongoDBMetadata(targetDsName, createIndexNum, createIndexTimeOut);
    }

    /**
     * 应用集群信息
     *
     * @param clusterInfoSet 集群信息集合
     * @param dbTableWhite   数据库表白名单
     */
    public void applyClusterInfo(Set<String> clusterInfoSet, String dbTableWhite) {

        if (clusterInfoSet.contains("0")) {
            deleteExistingTables(dbTableWhite);
        }

        if (clusterInfoSet.contains("1")) {
            printUserInfo();
        }

        if (clusterInfoSet.contains("2")) {
            synchronizeTableStructures(dbTableWhite);
        }

        if (clusterInfoSet.contains("6")) {
            synchronizeConfigSetting();
        }

        if (clusterInfoSet.contains("3")) {
            synchronizeTableIndexes(dbTableWhite);
        }

        if (clusterInfoSet.contains("4")) {
            enableLibrarySharding();
        }

        if (clusterInfoSet.contains("5")) {
            synchronizeShardKeys(dbTableWhite);
        }

        if (clusterInfoSet.contains("7")) {
            preSplitDatabaseTables(dbTableWhite);
        }
    }

    /**
     * 删除目标端已经存在的表
     *
     * @param dbTableWhite 数据库表白名单
     */
    private void deleteExistingTables(String dbTableWhite) {
        try {
            log.info("{} 开始删除目标端已经存在的表", workName);
            for (String ns : sourceMetadata.getNSList(dbTableWhite)) {
                applyMongoDBMetadata.dropTable(ns);
            }
        } catch (Exception e) {
            log.error("{} 删除目标端已经存在的表,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 输出用户信息
     */
    private void printUserInfo() {
        try {
            log.info("{} 开始输出用户信息", workName);
            sourceMetadata.printUserInfo();
        } catch (Exception e) {
            log.error("{} 输出所有用户信息失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 同步数据库表结构
     *
     * @param dbTableWhite 数据库表白名单
     */
    private void synchronizeTableStructures(String dbTableWhite) {
        try {
            log.info("{} 开始同步数据库表结构", workName);
            applyMongoDBMetadata.createCollection(sourceMetadata.getCollectionOptionMap(dbTableWhite));
            applyMongoDBMetadata.createView(sourceMetadata.getViewOptionMap(dbTableWhite));
        } catch (Exception e) {
            log.error("{} 同步数据库表结构失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 同步config.setting表
     */
    private void synchronizeConfigSetting() {
        try {
            log.info("{} 开始同步config.setting表", workName);
            applyMongoDBMetadata.updateConfigSetting(sourceMetadata.getConfigSettingList());
        } catch (Exception e) {
            log.error("{} 同步config.setting表失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 同步数据库表索引信息
     *
     * @param dbTableWhite 数据库表白名单
     */
    private void synchronizeTableIndexes(String dbTableWhite) {
        try {
            log.info("{} 开始同步数据库表索引信息", workName);
            applyMongoDBMetadata.createIndex(sourceMetadata.getIndexList(dbTableWhite));
        } catch (Exception e) {
            log.error("{} 同步数据库表索引信息失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 为所有库启用库分片
     */
    private void enableLibrarySharding() {
        try {
            log.info("{} 开始为所有库启用库分片", workName);
            applyMongoDBMetadata.enableShardingDataBase(sourceMetadata.getShardingDBNameList());
        } catch (Exception e) {
            log.error("{} 为所有库启用库分片失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 同步数据库表Shard Key信息
     *
     * @param dbTableWhite 数据库表白名单
     */
    private void synchronizeShardKeys(String dbTableWhite) {
        try {
            log.info("{} 开始同步数据库表Shard Key信息", workName);
            applyMongoDBMetadata.createShardKey(sourceMetadata.getShardKey(dbTableWhite));
        } catch (Exception e) {
            log.error("{} 同步数据库表Shard Key信息失败,错误信息:{}", workName, e.getMessage());
        }
    }

    /**
     * 预拆分数据库表
     *
     * @param dbTableWhite 数据库表白名单
     */
    private void preSplitDatabaseTables(String dbTableWhite) {
        try {
            log.info("{} 开始预拆分数据库表", workName);
            applyMongoDBMetadata.splitShardTable(sourceMetadata.getShardCollectionSplit(dbTableWhite));
        } catch (Exception e) {
            log.error("{} 预拆分数据库表失败,错误信息:{}", workName, e.getMessage());
        }
    }
}
