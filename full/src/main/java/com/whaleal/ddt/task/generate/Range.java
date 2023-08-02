package com.whaleal.ddt.task.generate;


import lombok.*;
import lombok.extern.log4j.Log4j2;

/**
 * @author: lhp
 * @time: 2021/7/19 5:02 下午
 * @desc: 数据分片范围
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
@Log4j2
public class Range {
    /**
     * 切分的字段名
     */
    private String columnName;
    /**
     * 最大值
     */
    private Object maxValue;
    /**
     * 最小值
     */
    private Object minValue;
    /**
     * 是否为边缘值
     * 否[min,max)
     * 是[min,max]
     */
    private boolean isMax = false;
    /**
     * 库表名
     */
    private String ns;
    /**
     * 查询范围条数
     */
    private int rangeSize;

}
