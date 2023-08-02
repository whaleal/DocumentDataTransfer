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
