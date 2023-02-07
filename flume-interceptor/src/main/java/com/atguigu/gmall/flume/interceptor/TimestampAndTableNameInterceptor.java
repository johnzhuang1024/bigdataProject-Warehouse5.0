package com.atguigu.gmall.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/7 21:02
 * @Version：1.0
 */
public class TimestampAndTableNameInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        Map<String, String> headers = event.getHeaders();
        String log = new String(event.getBody(), StandardCharsets.UTF_8);

        JSONObject jsonObject = JSONObject.parseObject(log);

        Long ts = jsonObject.getLong("ts");
        //Maxwell输出的数据中的ts字段时间戳单位为秒，Flume HDFSSink要求单位为毫秒
        String timeMills = String.valueOf(ts * 1000);

        String tableName = jsonObject.getString("table");

        headers.put("timestamp", timeMills);
        headers.put("tableName", tableName);
        return event;

    }

    @Override
    public List<Event> intercept(List<Event> events) {

        for (Event event : events) {
            intercept(event);
        }

        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {


        @Override
        public Interceptor build() {
            return new TimestampAndTableNameInterceptor ();
        }

        @Override
        public void configure(Context context) {

        }
    }
}

