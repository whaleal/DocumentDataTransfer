package com.whaleal.ddt.task;

import com.alibaba.fastjson2.JSON;
import com.whaleal.ddt.task.generate.Range;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Log4j2
public class SourceTaskInfo {
    /**
     * range
     */
    private Range range;
    /**
     * dbTableName
     */
    private String ns;
    /**
     * 源数据源名称
     */
    private String sourceDsName;
    /**
     * 目标数据源名称
     */
    private String targetDsName;
    /**
     * 开始时间
     */
    private long startTime;
    /**
     * 结束时间
     */
    private long endTime;

    public SourceTaskInfo(Range range, String ns, String sourceDsName) {
        this.range = range;
        this.ns = ns;
        this.sourceDsName = sourceDsName;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
