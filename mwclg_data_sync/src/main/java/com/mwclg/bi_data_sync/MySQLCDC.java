package com.mwclg.bi_data_sync;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQLCDC{
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        env.setParallelism(1);
        //checkpoint的一些配置
        env.enableCheckpointing(params.getInt("checkpointInterval",60000));
        env.getCheckpointConfig().setCheckpointTimeout(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(params.get("hostname", "127.0.0.1"))
                .port(params.getInt("port", 3306))
                .username(params.get("username", "root"))
                .password(params.get("password", ""))
                .serverTimeZone("Asia/Shanghai")
                //设置我们自己的实现
                .deserializer(new JsonStringDebeziumDeserializationSchema(8))
                .databaseList(params.get("databaseList", "cdc-test"))
                .tableList("cdc-test.source", "cdc-test.student")
                .startupOptions(StartupOptions.latest())
                .build();
        //使用 CDC Source 从 MySQL 读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        mysqlDS.printToErr("------>").setParallelism(1);

        env.execute(MySQLCDC.class.getSimpleName());

    }
}
