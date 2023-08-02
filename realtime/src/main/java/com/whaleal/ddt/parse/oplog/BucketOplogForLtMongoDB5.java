package com.whaleal.ddt.parse.oplog;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Set;

/**
 * 多线程操作解析Oplog。对应MongoDB版本5.x和6.x，为V3版本。
 * @author liheping
 */
@Log4j2
public class BucketOplogForLtMongoDB5 extends BucketOplog {

    public BucketOplogForLtMongoDB5(String workName, String dsName, int maxBucketNum, Set<String> ddlList, int ddlWait) {
        super(workName, dsName, maxBucketNum, ddlList, ddlWait);
    }

    /**
     * parseUpdate 解析更新数据
     *
     * @param document oplog数据
     * @desc 解析更新数据 。mongodb version 5.0以下的update的oplog处理
     */
    @Override
    public void parseUpdate(Document document) {
        String _id = ((Document) document.get("o2")).get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadataOplog.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        Document o2 = ((Document) document.get("o2"));
        Document o = (Document) document.get("o");
        o.remove("$v");
        // 有些oplog的o没有$set和$unset的为Replace
        if (o.get("$set") == null && o.get("$unset") == null) {
            // 是否开启upsert
            ReplaceOptions option = new ReplaceOptions();
            option.upsert(true);
            bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<Document>(o2, o, option));
        } else {
            bucketWriteModelListMap.get(bucketNum).add(new UpdateOneModel<Document>(o2, o));
        }
    }
}
