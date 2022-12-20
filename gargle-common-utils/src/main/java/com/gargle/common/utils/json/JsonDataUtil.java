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
        JSONObject jsonObject = new JSONObject();
        JSONArray array = new JSONArray();

        for (int i = 0; i < 5; i++) {
            JSONObject jsonObject1 = new JSONObject();
            jsonObject1.put("name" + i, i);
            jsonObject1.put("age" + i, i);
            array.add(jsonObject1);
        }

        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("name","sqw");
        jsonObject1.put("age", 16);

        jsonObject.put("array", array);
        jsonObject.put("A", "a");
        jsonObject.put("b", "b");
        jsonObject.put("info", jsonObject1);

        List<Entity> key = buildBig("key", jsonObject);
        System.out.println(JSONObject.toJSONString(key));
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

    public static void buildBigParam(String key, JSON json, List<Entity> entityList) {
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
                    for (int i = 0; i < array.size(); i++) {
                        JSONObject arrayJSONObject = array.getJSONObject(i);
                        buildA(
                                buildKey(key, entry.getKey() + "[" + i + "]"),
                                arrayJSONObject,
                                entityList
                        );
                    }
                } else if (entry.getValue() instanceof JSONObject) {
                    buildA(buildKey(key, entry.getKey()), (JSONObject) entry.getValue(), entityList);
                } else {
                    entityList.add(new Entity(key, entry.getKey(), getStringValue(entry.getValue())));
                }
            }
        } else if (json instanceof JSONArray){
            JSONArray array = (JSONArray) json;
            for (int i = 0; i < array.size(); i++) {
                JSONObject arrayJSONObject = array.getJSONObject(i);
                buildA(
                        buildKey(key, key + "[" + i + "]"),
                        arrayJSONObject,
                        entityList
                );
            }
        } else {
            entityList.add(new Entity(key, key, JSONObject.toJSONString(json)));
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
                    for (int i = 0; i < array.size(); i++) {
                        JSONObject arrayJSONObject = array.getJSONObject(i);
                        buildBigParam(
                                buildKey(key, entry.getKey() + "[" + i + "]"),
                                arrayJSONObject,
                                entityList
                        );
                    }
                } else if (entry.getValue() instanceof JSONObject) {
                    buildBigParam(buildKey(key, entry.getKey()), (JSONObject) entry.getValue(), entityList);
                } else {
                    entityList.add(new Entity(key, entry.getKey(), getStringValue(entry.getValue())));
                }
            }
        } else if (json instanceof JSONArray){
            JSONArray array = (JSONArray) json;
            for (int i = 0; i < array.size(); i++) {
                JSONObject arrayJSONObject = array.getJSONObject(i);
                buildBigParam(
                        buildKey(key, key + "[" + i + "]"),
                        arrayJSONObject,
                        entityList
                );
            }
        } else {
            entityList.add(new Entity(key, key, JSONObject.toJSONString(json)));
        }
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
