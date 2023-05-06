package com.mwclg.cdc_test;

import java.io.Serializable;

import com.mwclg.bean.DataLine;
import com.mwclg.utils.GetConfInfoUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import net.sf.json.JSONObject;
import net.sf.json.JSONArray;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.KafkaProducer;


public class DorisStreamLoad implements Serializable {

    public String getValue(String key){
        String value = GetConfInfoUtils.getProperty(key);
        return value;
    }

    private HttpClientBuilder httpClientBuilder = HttpClients.custom().setRedirectStrategy(new DefaultRedirectStrategy(){
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    });


    public void loadJson(DataLine dataLine, KafkaProducer<String, String> producer) throws IOException {
        String jsonData = dataLine.getData();
        String db = dataLine.getDb();
        String table = dataLine.getTable();
        String mergeType = dataLine.getMerge_type();

        String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
        String[] arr = jsonData.split("=-=-=");
        JSONArray jsonArray = new JSONArray();

        for (String line : arr) {
            try {
                JSONObject js = JSONObject.fromObject(line);
                jsonArray.add(js);
            } catch (Exception ex){
                System.out.println(line);
                System.out.println(ex);
            }
        }
        String jsonArrayStr = jsonArray.toString();
        CloseableHttpClient client = httpClientBuilder.build();
        String loadUrlStr = String.format(loadUrlPattern, getValue("doris_load_host"), db, table);

        try {
            HttpPut put = new HttpPut(loadUrlStr);
            put.removeHeaders(HttpHeaders.CONTENT_LENGTH);
            put.removeHeaders(HttpHeaders.TRANSFER_ENCODING);
            put.setHeader(HttpHeaders.EXPECT, "100-continue");
            put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader());
            String label = UUID.randomUUID().toString();
            // You can set stream load related properties in the Header, here we set label and column_separator.
            put.setHeader("label", label);
            put.setHeader("merge_type", mergeType);
            //      put.setHeader("two_phase_commit", "true");
            put.setHeader("column_separator", ",");
            put.setHeader("format", "json");
            put.setHeader("strip_outer_array", "true");
            put.setHeader("exec_mem_limit", "6442450944");
            StringEntity entity = new StringEntity(jsonArrayStr, "UTF-8");
            put.setEntity(entity);
        }finally {
            if (client != null){
                client.close();
            }
        }

    }

    public String basicAuthHeader(){
        String tobeEncode = getValue("doris_user") + ":" + getValue("doris_password");
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }
}
