package com.mwclg.cdc_test;


import com.mwclg.utils.GetConfInfoUtils;
import lombok.val;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import com.zbkj.util.KafkaUtil.proper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.HashMap;


public class CDCMysql2Doris {
    public String getValue(String key){
        String value = GetConfInfoUtils.getProperty(key);
        return value;
    }

    public void mysql_cdc_2_doris() throws Exception {
        Logger logger = LoggerFactory.getLogger(this.getClass().getName());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(4);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        /*checkpoint的相关设置*/
        // 启用检查点，指定触发checkpoint的时间间隔（单位：毫秒，默认500毫秒），默认情况是不开启的
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        // 设定Checkpoint超时时间，默认为10分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        /** 设定两个Checkpoint之间的最小时间间隔，防止出现例如状态数据过大而导致Checkpoint执行时间过长，从而导致Checkpoint积压过多
         * 最终Flink应用密切触发Checkpoint操作，会占用了大量计算资源而影响到整个应用的性能（单位：毫秒） */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60000);

        // 默认情况下，只有一个检查点可以运行
        // 根据用户指定的数量可以同时触发多个Checkpoint，进而提升Checkpoint整体的效率
        //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        /** 外部检查点
         * 不会在任务正常停止的过程中清理掉检查点数据，而是会一直保存在外部系统介质中，另外也可以通过从外部检查点中对任务进行恢复 */
        env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /** 如果有更近的保存点时，是否将作业回退到该检查点 */
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        // 设置可以允许的checkpoint失败数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        //设置可容忍的检查点失败数，默认值为0表示不允许容忍任何检查点失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);

        /**
         * 重启策略的配置
         */
        // 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L));

        /**
         * 获取同步表配置
         * database table
         */
        SingleOutputStreamOperator<Row> inputMysql = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://%s:%d/%s".format(getValue("doris_host"), Integer.parseInt(getValue("doris_port")), getValue("sync_config_db")))
                .setUsername(getValue("doris_user"))
                .setPassword(getValue("doris_password"))
                .setQuery("select member_id,sync_table from %s.%s".format(getValue("sync_config_db"), getValue("sync_config_table")))
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO))
                .finish()).uid("inputMysql");

        SingleOutputStreamOperator<String> databaseName = inputMysql.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row value) throws Exception {
                return value.getField(0).toString();
            }
        }).uid("databaseName");
        // 模糊监听
        SingleOutputStreamOperator<String> tableName = inputMysql.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row value) throws Exception {
                return value.getField(0).toString() + ".*";
            }
        }).uid("tableName");

//        val producer = KafkaUtil.getProducer;

        CloseableIterator<String> databaseIter = databaseName.executeAndCollect();
        String databaseList = StringUtils.join(databaseIter, ",");

        CloseableIterator<String> tableIter = tableName.executeAndCollect();
        String tableList = StringUtils.join(tableIter, ",");
        logger.info("databaseList:",databaseList);
        logger.info("tableList:",tableList);

        HashMap<String, Object> customConverterConfigs = new HashMap<>();
        customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");

        /**
         *
         * mysql source for doris
         */
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(getValue("rds_host"))
                .port(Integer.parseInt(getValue("rds_port")))
                .databaseList(databaseList)
                .tableList(tableList)
                .username(getValue("rds_user"))
                .password(getValue("rds_password"))
                .serverId("11110")
                .splitSize(Integer.parseInt(getValue("split_size")))
                .fetchSize(Integer.parseInt(getValue("fetch_size")))
                .startupOptions(StartupOptions.initial()) // 全量读取
                .includeSchemaChanges(true)
                // 发现新表，加入同步任务，需要在tableList中配置
                .scanNewlyAddedTableEnabled(true)
                .deserializer(new JsonDebeziumDeserializationSchema(false,customConverterConfigs))
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source");

        SingleOutputStreamOperator<String> ddlSqlStream = dataStreamSource.filter(line -> line.contains("historyRecord") && !line.contains("CHANGE COLUMN")).uid("ddlSqlStream");

        SingleOutputStreamOperator<String> dmlStream = dataStreamSource.filter(line -> !line.contains("historyRecord") && !line.contains("CHANGE COLUMN")).uid("dmlStream");


        DataStream<String> ddlDataStream = FlinkCDCBinLogETL.ddlFormat(ddlSqlStream);
        DataStream<Tuple4<String, String, String, String>> dmlDataStream = FlinkCDCBinLogETL.binLogETL(dmlStream);

        logger.debug(String.valueOf(ddlDataStream));

        //producer 为了在数据同步后通知分析任务
        ddlDataStream.addSink((SinkFunction<String>) new SinkSchema()).name("ALTER TABLE TO DORIS").uid("SinkSchema");
        dmlDataStream.addSink((SinkFunction<Tuple4<String, String, String, String>>) new SinkDoris()).name("Data TO DORIS").uid("SinkDoris");


        env.execute("Flink CDC Mysql To Doris With Initial");

    }

}
