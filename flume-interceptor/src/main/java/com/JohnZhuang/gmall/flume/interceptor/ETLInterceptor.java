package com.JohnZhuang.gmall.flume.interceptor;

import com.JohnZhuang.gmall.flume.utils.JSONUtil;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;


import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/5 20:02
 * @Version：1.0
 */
public class ETLInterceptor implements Interceptor {

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {

        //1、获取body当中的数据并转成字符串
        byte[] body = event.getBody();
        String log = new String(body, StandardCharsets.UTF_8);
        //2、判断字符串是否是一个合法的json，是：返回当前event；不是：返回null
        if (JSONUtil.isJSONValidate(log)) {
            return event;
        } else {
            return null;
        }
    }

    @Override
    public List<Event> intercept(List<Event> list) {

        Iterator<Event> iterator = list.iterator();

        while (iterator.hasNext()){
            Event next = iterator.next();
            if(intercept(next)==null){
                iterator.remove();
            }
        }

        return list;
    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }
        @Override
        public void configure(Context context) {

        }

    }

    @Override
    public void close() {

    }
}

