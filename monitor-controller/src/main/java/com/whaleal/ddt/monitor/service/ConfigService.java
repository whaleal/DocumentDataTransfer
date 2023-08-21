//package com.whaleal.ddt.monitor.service;
//
//
//import com.whaleal.mongot.model.ConfigEntity;
//
//import java.util.List;
//
///**
// * @author cc
// */
//public interface ConfigService {
//
//
//    /**
//     * 保存或修改配置
//     *
//     * @param configEntity 配置实体类
//     * @return 配置实体类
//     */
//    ConfigEntity saveConfig(ConfigEntity configEntity);
//
//    /**
//     * 检查任务名称是否存在
//     *
//     * @param taskName 配置信息的任务名称
//     * @return 是否重复
//     */
//    Boolean checkTaskName(String taskName);
//
//
//    /**
//     * 根据任务名称删除配置
//     *
//     * @param taskName 配置信息的任务名称
//     */
//    void deleteConfig(String taskName);
//
//
//    /**
//     * 模糊查询配置,根据任务名称
//     *
//     * @param name      配置信息的任务名称
//     * @param pageIndex 第几页
//     * @param pageSize  每页大小
//     * @return 配置集合
//     */
//    List<ConfigEntity> findConfig(String name, String url, Integer pageIndex, Integer pageSize);
//
//
//    /**
//     * 根据任务名查询单个配置
//     *
//     * @param taskName 配置信息的任务名称
//     * @return 配置实体
//     */
//    ConfigEntity findConfigByTaskName(String taskName);
//
//    /**
//     * 根据条件查询配置数
//     *
//     * @param name 配置信息的任务名称
//     * @param url  配置信息的sourceUrl 或 targetUrl
//     * @return 数量
//     */
//    long findConfigCount(String name, String url);
//}
