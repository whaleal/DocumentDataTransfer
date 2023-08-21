package com.whaleal.ddt.monitor.util;

import java.io.Serializable;
import java.util.List;
import java.util.Map;


/**
 * @author cc
 */
public class PageUtils implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * 总记录数
     */
    private Long totalCount;

    /**
     * 列表数据
     */
    private List<?> list;

    /**
     * 列表数据
     */
    private Map<String, Object> map;

    /**
     * 分页
     *
     * @param list       列表数据
     * @param totalCount 总记录数
     */
    public PageUtils(List<?> list, Long totalCount) {
        this.list = list;
        this.totalCount = totalCount;
    }

    /**
     * 分页
     *
     * @param totalCount 总记录数
     */
    public PageUtils(Map<String, Object> map, Long totalCount) {
        this.map = map;
        this.totalCount = totalCount;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public Map<String, Object> getMap() {
        return map;
    }

    public void setMap(Map<String, Object> map) {
        this.map = map;
    }

    public List<?> getList() {
        return list;
    }

    public void setList(List<?> list) {
        this.list = list;
    }

}
