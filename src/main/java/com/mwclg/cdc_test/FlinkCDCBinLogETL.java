package com.mwclg.cdc_test;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;

public class FlinkCDCBinLogETL {
    public static String matchColumnType(String columnType, Integer length, Integer scale){
        String returnColumnType = "VARCHAR(255)";
        switch (columnType){
            case "INT UNSIGNED": returnColumnType = "INT($length)";
            case "INT" : returnColumnType = "INT($length)";
            case "TINYINT" : returnColumnType = "TINYINT($length)";
            case "VARCHAR" : returnColumnType = "VARCHAR(${length * 3})";
            case "BIGINT" : returnColumnType = "BIGINT(${length})";
            case "TINYTEXT" : returnColumnType = "TINYTEXT";
            case "LONGTEXT" : returnColumnType = "STRING";
            case "TEXT" : returnColumnType = "STRING";
            case "DECIMAL" : returnColumnType = "DECIMAL($length,$scale)";
            case "VARBINARY" : returnColumnType = "STRING";
            case "TIMESTAMP" : returnColumnType = "STRING";
            case "ENUM" : returnColumnType = "TINYINT";
            case "MEDIUMINT" : returnColumnType = "INT";
            case "SMALLINT" : returnColumnType = "SMALLINT";
            case "MEDIUMTEXT" : returnColumnType = "STRING";
            default: returnColumnType = "STRING";

        }
        return returnColumnType;
    }

    public static DataStream<Tuple4<String,String,String,String>> binLogETL(DataStream dataStreamSource) {
        /**
         * 根据不同日志类型 匹配load doris方式
         */
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> tupleData = dataStreamSource.map(
                (MapFunction) line -> {
                    String data = null;
                    String mergetype = "APPEND";
                    JSONObject lineObj = JSONObject.fromObject(line);
                    JSONObject source = lineObj.getJSONObject("source");
                    String db = source.getString("db");
                    String table = source.getString("table");

                    if ("d" == lineObj.getString("op")) {
                        data = lineObj.getJSONObject("before").toString();
                    }else if ("u" == lineObj.getString("op")){
                        data = lineObj.getJSONObject("after").toString();
                        mergetype = "MERGE";
                    } else if ("c" == lineObj.getString("op")) {
                        data = lineObj.getJSONObject("after").toString();
                    } else if ("r" == lineObj.getString("op")) {
                        data = lineObj.getJSONObject("after").toString();
                        mergetype = "APPEND";
                    }
                    Tuple4<String, String, String, String> tuple4 = new Tuple4<>(mergetype, db, table, data);
                    return tuple4;
                }
        ).returns(TypeInformation.of(new TypeHint<Tuple4<String, String, String, String>>() { }));
        /**
         * 窗口聚合数据，将相同load方式，db,table的json 数据组合为长字符串，
         */
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> byKeyData = tupleData.keyBy(0, 1, 2)
                .timeWindow(Time.milliseconds(1000))
                .reduce((ReduceFunction<Tuple4<String, String, String, String>>) (value1, value2) -> new Tuple4(value1.f0, value1.f1, value1.f2, value2.f3 + "=-=-=" + value2.f3));

        return byKeyData;
    }

    public static DataStream<String> ddlFormat(DataStream<String> ddlDataStream){
        DataStream<String> ddlStrDataStream = ddlDataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                try {
                    JSONObject lineObj = JSONObject.fromObject(line);
                    JSONObject historyRecord = JSONObject.fromObject(lineObj.getString("historyRecord"));
                    JSONArray tableChangesArray = historyRecord.getJSONArray("tableChanges");
                    JSONObject tableChanges = JSONObject.fromObject(tableChangesArray.getJSONObject(0));
                    String tableChangeType = tableChanges.getString("type");
                    String ddlSql = "";

                    JSONObject table = tableChanges.optJSONObject("table");
                    String primaryKeyColumnNames = table.getString("primaryKeyColumnNames").replace("[", "").replace("]", "").replace("\"", "");
                    JSONArray columnsArray = table.getJSONArray("columns");

                    // 建表转换

                    if (tableChangeType.equals("CREATE")) {
                        String tableName = tableChanges.getString("id").replace("\"", "");
                        ArrayList<String> columnsArrayBuffer = new ArrayList<>();
                        for (Object data : columnsArray) {
                            JSONObject columnJson = JSONObject.fromObject(data);
                            String name = columnJson.getString("name");
                            String typeName = columnJson.getString("typeName");
                            Integer length = columnJson.optInt("length", 1);
                            Integer scale = columnJson.optInt("scale", 2);
                            String lastColumnType = matchColumnType(typeName, length, scale);
                            String lastColumn = name + " " + lastColumnType;
                            columnsArrayBuffer.add(lastColumn);
                        }
                        // 对列重新排序，主键依次放在最前面，避免错误Key columns should be a ordered prefix of the scheme
                        String[] keys = primaryKeyColumnNames.split(",");
                        for (int indexOfCol = 0; indexOfCol < keys.length; indexOfCol++) {
                            String col = keys[indexOfCol];
                            String columnFormat = "";
                            for (String column : columnsArrayBuffer) {
                                if (column.startsWith(col)) {
                                    columnFormat = column;
                                }
                            }
                            int index = columnsArrayBuffer.indexOf(columnFormat);
                            columnsArrayBuffer.remove(index);
                            columnsArrayBuffer.add(indexOfCol, columnFormat);
                        }
                        String header = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
                        String end = ") UNIQUE KEY(" + primaryKeyColumnNames + ")" + "DISTRIBUTED BY HASH(" + primaryKeyColumnNames + ")  BUCKETS 10  PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\")";
                        ddlSql = header + StringUtils.join(columnsArrayBuffer.toArray(), ",") + end;
                    } else if (tableChangeType == "ALTER") {
                        String ddl = historyRecord.getString("ddl").replace("\r\n", " ");
                        System.out.println(ddl);
                        if (ddl.startsWith("RENAME")) {
                            ddl = ddl.replace("`", "");
                            String[] arr = ddl.split("");
                            ddlSql = "ALTER TABLE " + arr[2] + " RENAME " + arr[4];
                        } else if (ddl.contains("DROP COLUMN")) {
                            ddlSql = ddl;
                        } else {
                            String dbTableName = tableChanges.getString("id").replace("\"", "");
                            String addColName = ddl.split(" ")[5].replace("`", "");
                            String colTpe = "";
                            for (int lineInx = 0; lineInx < columnsArray.size(); lineInx++) {
                                JSONObject columnJson = JSONObject.fromObject(lineInx);
                                if (columnJson.getString("name") == addColName) {
                                    String typeName = columnJson.getString("typeName");
                                    int length = columnJson.optInt("length", 1);
                                    int scale = columnJson.optInt("scale", 2);
                                    colTpe = matchColumnType(typeName, length, scale);
                                }
                            }

                            if (ddl.contains("ADD COLUMN")) {
                                ddlSql = "ALTER TABLE " + dbTableName + " ADD COLUMN " + addColName + " " + colTpe;
                            } else {
                                ddlSql = "ALTER TABLE " + dbTableName + " MODIFY COLUMN " + addColName + " " + colTpe;
                            }
                        }
                    }
                    System.out.println(ddlSql);
                    return ddlSql;

                } catch (Exception ex) {
                    System.out.println("select 1");
                    System.out.println(ex);
                }
                return line;
            }
        });
        return ddlStrDataStream;
    }
}
