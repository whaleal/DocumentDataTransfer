package com.whaleal.ddt.metadata.util;

import com.mongodb.client.model.*;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * todo gpt优化的代码 不知道正确与否
 */
@Log4j2
public class WriteModelUtil {

    //    /**
//     * writeModel转为String
//     *
//     * @param writeModel writeModel
//     * @return String
//     */
//    public static String writeModelToString(WriteModel writeModel) {
//        StringBuilder stringBuilder = new StringBuilder();
//        try {
//            stringBuilder.append("type").append(":").append(writeModel.getClass().getSimpleName()).append(",");
//            if (writeModel instanceof InsertOneModel) {
//                Document insertDoc = ((InsertOneModel<Document>) writeModel).getDocument();
//                stringBuilder.append(insertDoc.toJson());
//            } else if (writeModel instanceof ReplaceOneModel) {
//                Bson filter = ((ReplaceOneModel<Document>) writeModel).getFilter();
//                Document replacement = ((ReplaceOneModel<Document>) writeModel).getReplacement();
//                stringBuilder.append("filter").append(":").append(filter.toString()).append(",");
//                stringBuilder.append("replacement").append(":").append(replacement.toJson());
//            } else if (writeModel instanceof DeleteManyModel) {
//                Bson deleteManyFilterDoc = ((DeleteManyModel<Document>) writeModel).getFilter();
//                stringBuilder.append("filter").append(":").append(deleteManyFilterDoc.toString());
//            } else if (writeModel instanceof DeleteOneModel) {
//                Bson deleteFilterDoc = ((DeleteOneModel<Document>) writeModel).getFilter();
//                stringBuilder.append("filter").append(":").append(deleteFilterDoc.toString());
//            } else if (writeModel instanceof UpdateOneModel) {
//                Bson updateFilterDoc = ((UpdateOneModel<Document>) writeModel).getFilter();
//                Bson update = ((UpdateOneModel<Document>) writeModel).getUpdate();
//                stringBuilder.append("filter").append(":").append(updateFilterDoc.toString()).append(",");
//                stringBuilder.append("replacement").append(":").append(update.toString());
//            } else if (writeModel instanceof UpdateManyModel) {
//                Bson updateFilterDoc = ((UpdateManyModel<Document>) writeModel).getFilter();
//                Bson update = ((UpdateManyModel<Document>) writeModel).getUpdate();
//                stringBuilder.append("filter").append(":").append(updateFilterDoc.toString()).append(",");
//                stringBuilder.append("replacement").append(":").append(update.toString());
//            } else {
//                stringBuilder.append("unKnow").append(":").append(writeModel.toString());
//            }
//        } catch (Exception e) {
//            stringBuilder.append(",");
//            try {
//                stringBuilder.append(writeModel.toString()).append(",");
//            } catch (Exception ignored) {
//            }
//            stringBuilder.append(e.getMessage());
//        }
//        return stringBuilder.toString();
//    }

    /**
     * 将WriteModel实例转换为格式化的字符串表示。
     *
     * @param writeModel 要转换的WriteModel实例。
     * @return 包含WriteModel类型和内容的字符串表示。
     */
    public static String writeModelToString(WriteModel<?> writeModel) {
        StringBuilder resultStringBuilder = new StringBuilder();
        try {
            // 添加WriteModel的类型（类名）到结果字符串中。
            resultStringBuilder.append("type").append(":").append(writeModel.getClass().getSimpleName()).append(",");

            // 确定WriteModel的类型并调用相应的处理方法。
            if (writeModel instanceof InsertOneModel) {
                handleInsertOneModel(resultStringBuilder, (InsertOneModel<?>) writeModel);
            } else if (writeModel instanceof ReplaceOneModel) {
                handleReplaceOneModel(resultStringBuilder, (ReplaceOneModel<?>) writeModel);
            } else if (writeModel instanceof DeleteManyModel) {
                handleDeleteManyModel(resultStringBuilder, (DeleteManyModel<?>) writeModel);
            } else if (writeModel instanceof DeleteOneModel) {
                handleDeleteOneModel(resultStringBuilder, (DeleteOneModel<?>) writeModel);
            } else if (writeModel instanceof UpdateOneModel) {
                handleUpdateOneModel(resultStringBuilder, (UpdateOneModel<?>) writeModel);
            } else if (writeModel instanceof UpdateManyModel) {
                handleUpdateManyModel(resultStringBuilder, (UpdateManyModel<?>) writeModel);
            } else {
                // 处理未知的WriteModel类型。
                resultStringBuilder.append("unknown").append(":").append(writeModel.toString());
            }
        } catch (Exception e) {
            // 处理处理过程中可能发生的异常。
            resultStringBuilder.append(",").append(writeModel.toString()).append(",");
            resultStringBuilder.append("出现错误：").append(e.getMessage());
        }
        return resultStringBuilder.toString();
    }

    /**
     * 处理InsertOneModel，并将其内容（Document）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param insertOneModel      要处理的InsertOneModel。
     */
    private static void handleInsertOneModel(StringBuilder resultStringBuilder, InsertOneModel<?> insertOneModel) {
        // 从InsertOneModel中提取Document，并将其以JSON形式附加到结果中。
        Document insertDoc = (Document) insertOneModel.getDocument();
        resultStringBuilder.append(insertDoc.toJson());
    }

    /**
     * 处理ReplaceOneModel，并将其过滤器和替换内容（Documents）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param replaceOneModel     要处理的ReplaceOneModel。
     */
    private static void handleReplaceOneModel(StringBuilder resultStringBuilder, ReplaceOneModel<?> replaceOneModel) {
        // 从ReplaceOneModel中提取过滤器和替换内容（Documents），并将它们以JSON形式附加到结果中。
        Bson filter = (Bson) replaceOneModel.getFilter();
        Document replacement = (Document) replaceOneModel.getReplacement();
        resultStringBuilder.append("filter").append(":").append(filter.toString()).append(",");
        resultStringBuilder.append("replacement").append(":").append(replacement.toJson());
    }

    /**
     * 处理DeleteManyModel，并将其过滤器（Bson）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param deleteManyModel     要处理的DeleteManyModel。
     */
    private static void handleDeleteManyModel(StringBuilder resultStringBuilder, DeleteManyModel<?> deleteManyModel) {
        // 从DeleteManyModel中提取过滤器（Bson），并将其以字符串形式附加到结果中。
        Bson deleteManyFilterDoc = (Bson) deleteManyModel.getFilter();
        resultStringBuilder.append("filter").append(":").append(deleteManyFilterDoc.toString());
    }

    /**
     * 处理DeleteOneModel，并将其过滤器（Bson）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param deleteOneModel      要处理的DeleteOneModel。
     */
    private static void handleDeleteOneModel(StringBuilder resultStringBuilder, DeleteOneModel<?> deleteOneModel) {
        // 从DeleteOneModel中提取过滤器（Bson），并将其以字符串形式附加到结果中。
        Bson deleteFilterDoc = (Bson) deleteOneModel.getFilter();
        resultStringBuilder.append("filter").append(":").append(deleteFilterDoc.toString());
    }

    /**
     * 处理UpdateOneModel，并将其过滤器和更新内容（Bson）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param updateOneModel      要处理的UpdateOneModel。
     */
    private static void handleUpdateOneModel(StringBuilder resultStringBuilder, UpdateOneModel<?> updateOneModel) {
        // 从UpdateOneModel中提取过滤器和更新内容（Bson），并将它们以JSON形式附加到结果中。
        Bson updateFilterDoc = (Bson) updateOneModel.getFilter();
        Bson update = (Bson) updateOneModel.getUpdate();
        resultStringBuilder.append("filter").append(":").append(updateFilterDoc.toString()).append(",");
        resultStringBuilder.append("replacement").append(":").append(update.toString());
    }

    /**
     * 处理UpdateManyModel，并将其过滤器和更新内容（Bson）附加到结果中。
     *
     * @param resultStringBuilder 用于附加内容的StringBuilder。
     * @param updateManyModel     要处理的UpdateManyModel。
     */
    private static void handleUpdateManyModel(StringBuilder resultStringBuilder, UpdateManyModel<?> updateManyModel) {
        // 从UpdateManyModel中提取过滤器和更新内容（Bson），并将它们以JSON形式附加到结果中。
        Bson updateFilterDoc = (Bson) updateManyModel.getFilter();
        Bson update = (Bson) updateManyModel.getUpdate();
        resultStringBuilder.append("filter").append(":").append(updateFilterDoc.toString()).append(",");
        resultStringBuilder.append("replacement").append(":").append(update.toString());
    }

}
