package com.gargle.common.utils.config;

import com.gargle.common.config.GargleConfig;
import com.gargle.common.exception.GargleException;
import com.gargle.common.utils.string.StringUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * ClassName:ConfigUtil
 * Description:
 *
 * @author qingwen.shang
 * @email shangqaq@163.com
 * @date 2022/11/29 17:39
 */
public final class ConfigUtil {

    /**
     * 解析 properties
     *
     * @param properties 格式 k1=v1;k2=v2;......
     */
    public static Map<String, String> buildProperties(String properties) {
        if (StringUtil.isBlank(properties)) {
            return null;
        }

        properties = properties.trim();
        String[] s1 = properties.split(";");
        if (s1.length == 0) {
            return null;
        }
        Map<String, String> map = new HashMap<>();
        for (String str : s1) {
            if (StringUtil.isBlank(str)) {
                continue;
            }
            str = str.trim();
            String[] s2 = str.split("=");
            if (s2.length != 2) {
                throw new GargleException("Build properties 格式化异常! properties: " + properties);
            }

            map.put(s2[0], s2[1]);
        }

        if (map.size() == 0) {
            return null;
        }
        return map;
    }

    public static String buildConnectionProperties(GargleConfig.DatasourceConfig druidDatasource) {
        Map<String, String> config = buildProperties(druidDatasource.getConnectionProperties());
        if (config == null) {
            config = new HashMap<>();
        }

        if (!config.containsKey("config.decrpt")) {
            config.put("config.decrpt", String.valueOf(druidDatasource.isDecrpt()));
        }

        if (!config.containsKey("druid.stat.logSlowSql")) {
            config.put("druid.stat.logSlowSql", String.valueOf(druidDatasource.isLogSlowSql()));
        }

        if (!config.containsKey("druid.stat.slowSqlMillis") && druidDatasource.isLogSlowSql()) {
            if (druidDatasource.getSlowSqlMillis() <= 0) {
                throw new GargleException("参数 slowSqlMillis 配置异常, 需要 大于 0");
            }
            config.put("druid.stat.slowSqlMillis", String.valueOf(druidDatasource.getSlowSqlMillis()));
        }

        if (!config.containsKey("druid.stat.mergeSql")) {
            config.put("druid.stat.mergeSql", String.valueOf(druidDatasource.isMergeSql()));
        }

        return buildConfig(config);
    }

    private static String buildConfig(Map<String, String> config) {
        if (config == null || config.size() == 0) {
            return "";
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            stringBuilder.append(entry.getKey()).append("=").append(entry.getValue()).append(";");
        }

        return stringBuilder.toString();
    }
}
