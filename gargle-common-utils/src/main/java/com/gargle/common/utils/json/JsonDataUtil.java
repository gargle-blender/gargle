package com.gargle.common.utils.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.gargle.common.utils.string.StringUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ClassName:BigParamsUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/04/26 18:14
 */
public class JsonDataUtil {

    private static final Logger logger = LoggerFactory.getLogger(JsonDataUtil.class);

    public static void main(String[] args) {
        JSONArray data = new JSONArray();

        JSONArray array1 = new JSONArray();
        JSONArray array11 = new JSONArray();
        JSONArray array12= new JSONArray();
        for (int i = 0; i < 2; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", "sqw" + i);
            jsonObject.put("age", i);
            array11.add(jsonObject);
//            data.add(jsonObject);

            JSONObject jsonObject2 = new JSONObject();
            jsonObject2.put("name", "sqw" + i);
            jsonObject2.put("age", i);
            array12.add(jsonObject2);
//            data.add(jsonObject2);
        }

        array1.add(array11);
        array1.add(array12);

        data.add(array1);
//        data.add("1");
//        data.add("2");
//        data.add("3");

        System.out.println(data.toJSONString());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "sqw");
        jsonObject.put("age", 18);

        System.out.println(JSONObject.toJSONString(buildBig("S_S_TESTFIELD", data)));
    }


    public static List<Entity> buildBig(String key, Object value){
        if (value == null){
            return null;
        }
        try {
            JSON json = (JSON) JSON.parse(String.valueOf(value));
            List<Entity> list = new ArrayList<>();
            buildBigParam(key, json, list);
            return list;
        } catch (Exception e){
            logger.error("大字段Json格式化异常! key: {}, value: {}", key, value, e);
            return null;
        }
    }

    private static void buildBigParam(String key, JSON json, List<Entity> entityList) {
        if (json == null){
            return;
        }
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                if (StringUtil.isBlank(entry.getKey())) {
                    continue;
                }
                if (entry.getValue() instanceof JSONArray) {
                    JSONArray array = (JSONArray) entry.getValue();
                    buildArray(key, entityList, array);
                } else if (entry.getValue() instanceof JSONObject) {
                    buildA(buildKey(key, entry.getKey()), (JSONObject) entry.getValue(), entityList);
                } else {
                    entityList.add(new Entity(key, entry.getKey(), getStringValue(entry.getValue())));
                }
            }
        } else if (json instanceof JSONArray){
            JSONArray array = (JSONArray) json;
            buildArray(key, entityList, array);
        } else {
            entityList.add(new Entity(key, key, JSONObject.toJSONString(json)));
        }
    }

    private static void buildArray(String key, List<Entity> entityList, JSONArray array) {
        for (int i = 0; i < array.size(); i++) {
            Object obj = array.get(i);
            if (obj instanceof JSONObject){
                buildA(
                        buildKey("", key + "[" + i + "]"),
                        (JSONObject)obj,
                        entityList
                );
            } else if (obj instanceof JSONArray){
                buildA(
                        buildKey("", key + "[" + i + "]"),
                        (JSONArray)obj,
                        entityList
                );
            } else {
                entityList.add(new Entity(key + "[" + i + "]", "", getStringValue(obj)));
            }
        }
    }

    /**
     * buildBigParam 递归缓冲.
     *
     * @param key
     * @param json
     * @param entityList
     */
    private static void buildA(String key, JSON json, List<Entity> entityList) {
        buildBigParam(key, json, entityList);
    }

    private static String getStringValue(Object obj) {
        return String.valueOf(obj == null ? "" : obj);
    }

    private static String buildKey(String key, String next) {
        if (StringUtil.isBlank(key)) {
            return next;
        }
        return key + "|" + next;
    }

    private static String buildPointer(String pointer, Integer next) {
        if (pointer == null || StringUtil.isBlank(pointer)) {
            return String.valueOf(next);
        }
        return pointer + "." + next;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Entity {
        private String index;

        private String paramName;

        private String paramValue;
    }
}
