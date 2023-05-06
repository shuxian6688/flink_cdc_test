package com.mwclg.bi_data_sync;

import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.utils.TemporalConversions;
import io.debezium.data.Envelope;
import io.debezium.time.Date;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonStringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord record, Collector out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            String insert = extractAfterRow(value, valueSchema);
            out.collect(new Tuple2<>(true, insert));
        } else if (op == Envelope.Operation.DELETE) {
            String delete = extractBeforeRow(value, valueSchema);
            out.collect(new Tuple2<>(false, delete));
        }else {
            String after = extractAfterRow(value, valueSchema);
            out.collect(new Tuple2<>(true, after));
        }
    }

    private String extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        Map<String,Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }
    private String extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        Map<String,Object> rowMap = getRowMap(after);
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(rowMap);
    }


    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple2<Boolean,String>>(){});
    }


    public JsonStringDebeziumDeserializationSchema(int zoneOffset) {
        //实现一个用于转换时间的Converter
        this.runtimeConverter = (dbzObj,schema) -> {
            if(schema.name() != null){
                switch (schema.name()) {
                    case Timestamp.SCHEMA_NAME:
                        return TimestampData.fromEpochMillis((Long) dbzObj).toLocalDateTime().atOffset(ZoneOffset.ofHours(zoneOffset)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    case MicroTimestamp.SCHEMA_NAME:
                        long micro = (long) dbzObj;
                        return TimestampData.fromEpochMillis(micro / 1000, (int) (micro % 1000 * 1000)).toLocalDateTime().atOffset(ZoneOffset.ofHours(zoneOffset)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    case NanoTimestamp.SCHEMA_NAME:
                        long nano = (long) dbzObj;
                        return TimestampData.fromEpochMillis(nano / 1000_000, (int) (nano % 1000_000)).toLocalDateTime().atOffset(ZoneOffset.ofHours(zoneOffset)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                    case Date.SCHEMA_NAME:
                        return TemporalConversions.toLocalDate(dbzObj).format(DateTimeFormatter.ISO_LOCAL_DATE);
                }
            }
            return dbzObj;
        };
    }

    //定义接口
    private interface DeserializationRuntimeConverter extends Serializable {
        Object convert(Object dbzObj, Schema schema);
    }

    private final JsonStringDebeziumDeserializationSchema.DeserializationRuntimeConverter runtimeConverter;


    private Map<String,Object> getRowMap(Struct after){
        //转换时使用对应的转换器
        return after.schema().fields().stream()
                .collect(Collectors.toMap(Field::name,f->runtimeConverter.convert(after.get(f),f.schema())));
    }

}