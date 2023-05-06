package com.mwclg.cdc_test;

import com.mwclg.bean.DataLine;
import com.mwclg.utils.KafkaUtil;
import org.apache.flink.api.java.tuple.Tuple4;
import net.sf.json.JSONObject;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;


public class SinkDoris{

    public static class sinkDoris implements SinkFunction<Tuple4<String, String, String, String>>{
        @Override
        public void invoke(Tuple4<String, String, String, String> value, Context context) throws Exception {
            DorisStreamLoad dorisStreamLoad = new DorisStreamLoad();
            FlinkKafkaProducer<String> cdc_default_topic = KafkaUtil.getKafkaProducer("CDC_DEFAULT_TOPIC");

            DataLine dataLine = new DataLine(value.f3,value.f1,value.f0,value.f2);

//            dorisStreamLoad.loadJson(dataLine,producer);
//            val producer = KafkaUtil.getProducer;
//            JSONObject json = new JSONObject();
//            json.put("db",value.f2);
//            json.put("table",value.f3);
//            KafkaUtil.sendKafkaMsg(producer, json.toString, "sync_table");
        }
    }

}

