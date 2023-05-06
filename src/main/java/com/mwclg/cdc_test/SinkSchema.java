package com.mwclg.cdc_test;

import com.mwclg.utils.SQLManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkSchema {
    public class sinkSchema extends RichSinkFunction<String> {
        Connection conn ;
        PreparedStatement ps ;
//        MysqlPool mysqlPool  = "";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            mysqlPool = MysqlManager.getMysqlPool;
            conn = SQLManager.getConnection();
            conn.setAutoCommit(false);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (conn != null) {
                conn.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        @Override
        public void invoke(String sql, Context context) throws Exception {
            super.invoke(sql, context);
            if (sql !="" && sql.isEmpty()){
                ps = conn.prepareStatement(sql);
                try {
                    ps.execute();
                }catch( Exception ex){
                    System.out.println(ex);
                }
                conn.commit();
            }
        }
    }
}
