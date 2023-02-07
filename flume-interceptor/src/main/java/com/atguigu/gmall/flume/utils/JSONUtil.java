package com.atguigu.gmall.flume.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONException;

/**
 * @Description:
 * @Author: JohnZhuang1024
 * @Date: 2023/2/5 20:01
 * @Version：1.0
 */
public class JSONUtil {
    /*
     * 通过异常判断是否是json字符串
     * 是：返回true  不是：返回false
     * */
    public static boolean isJSONValidate(String log){
        try {
            JSONObject.parseObject(log);
            return true;
        }catch (JSONException e){
            return false;
        }
    }
}

