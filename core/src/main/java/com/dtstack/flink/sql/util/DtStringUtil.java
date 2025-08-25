/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.enums.ColumnType;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class DtStringUtil {

    private static final Pattern NO_VERSION_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter) {
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else if (c == '(' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == ')' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    public static List<String> splitField(String str) {
        final char delimiter = ',';
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        int idx = 0;
        for (char c : chars) {
            char flag = 0;
            if (idx > 0) {
                flag = chars[idx - 1];
            }
            if (c == delimiter) {
                if (inQuotes) {
                    b.append(c);
                } else if (inSingleQuotes) {
                    b.append(c);
                } else if (bracketLeftNum > 0) {
                    b.append(c);
                } else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            } else if (c == '\"' && '\\' != flag && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(c);
            } else if (c == '\'' && '\\' != flag && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            } else if (c == '(' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == ')' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else if (c == '<' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum++;
                b.append(c);
            } else if (c == '>' && !inSingleQuotes && !inQuotes) {
                bracketLeftNum--;
                b.append(c);
            } else {
                b.append(c);
            }
            idx++;
        }

        tokensList.add(b.toString());

        return tokensList;
    }

    public static String replaceIgnoreQuota(String str, String oriStr, String replaceStr){
        String splitPatternStr = oriStr + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?=(?:[^']*'[^']*')*[^']*$)";
        return str.replaceAll(splitPatternStr, replaceStr);
    }

    /**
     * 处理 sql 中 "--" 注释，而不删除引号内的内容
     *
     * @param sql 解析出来的 sql
     * @return 返回无注释内容的 sql
     */
    public static String dealSqlComment(String sql) {
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        StringBuilder b = new StringBuilder(sql.length());
        char[] chars = sql.toCharArray();
        for (int index = 0; index < chars.length; index ++) {
            StringBuilder tempSb = new StringBuilder(2);
            if (index >= 1) {
                tempSb.append(chars[index - 1]);
                tempSb.append(chars[index]);
            }

            if ("--".equals(tempSb.toString())) {
                if (inQuotes) {
                    b.append(chars[index]);
                } else if (inSingleQuotes) {
                    b.append(chars[index]);
                } else {
                    b.deleteCharAt(b.length() - 1);
                    while (chars[index] != '\n') {
                        // 判断注释内容是不是行尾或者 sql 的最后一行
                        if (index == chars.length - 1) {
                            break;
                        }
                        index++;
                    }
                }
            } else if (chars[index] == '\"' && '\\' != chars[index] && !inSingleQuotes) {
                inQuotes = !inQuotes;
                b.append(chars[index]);
            } else if (chars[index] == '\'' && '\\' != chars[index] && !inQuotes) {
                inSingleQuotes = !inSingleQuotes;
                b.append(chars[index]);
            } else {
                b.append(chars[index]);
            }
        }
        return b.toString();
    }

    public static String col2string(Object column, String type) {
        String rowData = column.toString();
        ColumnType columnType = ColumnType.valueOf(type.toUpperCase());
        Object result = null;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData);
                break;
            case SMALLINT:
                result = Short.valueOf(rowData);
                break;
            case INT:
                result = Integer.valueOf(rowData);
                break;
            case BIGINT:
                result = Long.valueOf(rowData);
                break;
            case FLOAT:
                result = Float.valueOf(rowData);
                break;
            case DOUBLE:
                result = Double.valueOf(rowData);
                break;
            case DECIMAL:
                result = new BigDecimal(rowData);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                result = rowData;
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData);
                break;
            case DATE:
                result = DateUtil.dateToString((java.util.Date)column);
                break;
            case TIME:
                result = DateUtil.getTimeFromStr(String.valueOf(column));
                break;
            case TIMESTAMP:
                result = DateUtil.timestampToString((java.util.Date)column);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return result.toString();
    }

    public static String getPluginTypeWithoutVersion(String engineType) {
        Preconditions.checkNotNull(engineType, "type can't be null!");

        Matcher matcher = NO_VERSION_PATTERN.matcher(engineType);

        if (!matcher.find()) {
            return engineType;
        }

        return matcher.group(1);
    }

    /**
     * add specify params to dbUrl
     * @param dbUrl
     * @param addParams
     * @param isForce true:replace exists param
     * @return
     */
    public static String addJdbcParam(String dbUrl, Map<String, String> addParams, boolean isForce){

        if(Strings.isNullOrEmpty(dbUrl)){
            throw new RuntimeException("dburl can't be empty string, please check it.");
        }

        if(addParams == null || addParams.size() == 0){
            return dbUrl;
        }

        String[] splits = dbUrl.split("\\?");
        String preStr = splits[0];
        Map<String, String> params = Maps.newHashMap();
        if(splits.length > 1){
            String existsParamStr = splits[1];
            String[] existsParams = StringUtils.split(existsParamStr, "&");
            for(String oneParam : existsParams){
                String[] kv = StringUtils.split(oneParam, "=");
                if(kv.length != 2){
                    throw new RuntimeException("illegal dbUrl:" + dbUrl);
                }

                params.put(kv[0], kv[1]);
            }
        }

        for(Map.Entry<String, String> addParam : addParams.entrySet()){
            if(!isForce && params.containsKey(addParam.getKey())){
                continue;
            }

            params.put(addParam.getKey(), addParam.getValue());
        }

        //rebuild dbURL
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for(Map.Entry<String, String> param : params.entrySet()){
            if(!isFirst){
                sb.append("&");
            }

            sb.append(param.getKey()).append("=").append(param.getValue());
            isFirst = false;
        }

        return preStr + "?" + sb.toString();
    }

    public static boolean isJson(String str) {
        boolean flag = false;
        if (StringUtils.isNotBlank(str)) {
            try {
                objectMapper.readValue(str, Map.class);
                flag = true;
            } catch (Throwable e) {
                flag = false;
            }
        }
        return flag;
    }

    public static Object parse(String str,Class clazz){
        String fieldType = clazz.getName();
        Object object = null;
        if(fieldType.equals(Integer.class.getName())){
            object = Integer.parseInt(str);
        }else if(fieldType.equals(Long.class.getName())){
            object = Long.parseLong(str);
        }else if(fieldType.equals(Byte.class.getName())){
            object = str.getBytes()[0];
        }else if(fieldType.equals(String.class.getName())){
            object = str;
        }else if(fieldType.equals(Float.class.getName())){
            object = Float.parseFloat(str);
        }else if(fieldType.equals(Double.class.getName())){
            object = Double.parseDouble(str);
        }else if (fieldType.equals(Timestamp.class.getName())){
            object = Timestamp.valueOf(str);
        }else{
            throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
        }
        return object;
    }


    public static String firstUpperCase(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static String getTableFullPath(String schema, String tableName) {
        String[] tableInfoSplit = StringUtils.split(tableName, ".");
        //表明表信息带了schema
        if(tableInfoSplit.length == 2){
            schema = tableInfoSplit[0];
            tableName = tableInfoSplit[1];
        }

        //清理首个字符" 和最后字符 "
        schema = rmStrQuote(schema);
        tableName = rmStrQuote(tableName);

        if (StringUtils.isEmpty(schema)){
            return addQuoteForStr(tableName);
        }

        return addQuoteForStr(schema) + "." + addQuoteForStr(tableName);
    }

    /**
     * 清理首个字符" 和最后字符 "
     */
    public static String rmStrQuote(String str){
        if(StringUtils.isEmpty(str)){
            return str;
        }

        if(str.startsWith("\"")){
            str = str.substring(1);
        }

        if(str.endsWith("\"")){
            str = str.substring(0, str.length()-1);
        }

        return str;
    }

    public static String addQuoteForStr(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    public static String getStartQuote() {
        return "\"";
    }

    public static String getEndQuote() {
        return "\"";
    }

    public static String removeStartAndEndQuota(String str) {
        String removeStart = StringUtils.removeStart(str, "'");
        return StringUtils.removeEnd(removeStart, "'");
    }

    /**
     * 判断当前对象是null 还是空格
     *
     * @param obj 需要判断的对象
     * @return 返回true 如果对象是空格或者为null
     */
    public static boolean isEmptyOrNull(Object obj) {
        return Objects.isNull(obj) || obj.toString().isEmpty();
    }

    /**
     * 将特定格式的数据结构字符串转换为JSON格式
     * 支持解析包含FileInfo对象的复合数据结构
     *
     * @param dataStructureStr 待转换的数据结构字符串，格式如: (uniscid=value, ywid=value, fileList=[FileInfo(...), FileInfo(...)], ...)
     * @return JSON格式字符串
     */
    public static String convertDataStructureToJson(String dataStructureStr) {
        if (isEmptyOrNull(dataStructureStr)) {
            return "{}";
        }

        try {
            // 移除最外层的括号
            String content = dataStructureStr.trim();
            if (content.startsWith("(") && content.endsWith(")")) {
                content = content.substring(1, content.length() - 1);
            }

            Map<String, Object> result = new HashMap<>();
            List<String> fields = parseFields(content);

            for (String field : fields) {
                if (isEmptyOrNull(field)) {
                    continue;
                }

                String[] keyValue = parseKeyValue(field);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();

                    if (key.equals("fileList") && value.startsWith("[") && value.endsWith("]")) {
                        // 处理fileList数组
                        result.put(key, parseFileList(value));
                    } else {
                        // 处理普通字段，移除空值标记
                        result.put(key, value.isEmpty() ? null : value);
                    }
                }
            }

            return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert data structure to JSON: " + e.getMessage(), e);
        }
    }

    /**
     * 解析字段列表，考虑嵌套的括号和方括号
     */
    private static List<String> parseFields(String content) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        int parenthesesCount = 0;
        int bracketsCount = 0;
        boolean inQuotes = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                currentField.append(c);
            } else if (!inQuotes) {
                if (c == '(') {
                    parenthesesCount++;
                    currentField.append(c);
                } else if (c == ')') {
                    parenthesesCount--;
                    currentField.append(c);
                } else if (c == '[') {
                    bracketsCount++;
                    currentField.append(c);
                } else if (c == ']') {
                    bracketsCount--;
                    currentField.append(c);
                } else if (c == ',' && parenthesesCount == 0 && bracketsCount == 0) {
                    // 找到字段分隔符
                    fields.add(currentField.toString().trim());
                    currentField = new StringBuilder();
                } else {
                    currentField.append(c);
                }
            } else {
                currentField.append(c);
            }
        }

        // 添加最后一个字段
        if (currentField.length() > 0) {
            fields.add(currentField.toString().trim());
        }

        return fields;
    }

    /**
     * 解析键值对
     */
    private static String[] parseKeyValue(String field) {
        int equalIndex = field.indexOf('=');
        if (equalIndex > 0) {
            String key = field.substring(0, equalIndex).trim();
            String value = field.substring(equalIndex + 1).trim();
            return new String[]{key, value};
        }
        return new String[]{field.trim(), ""};
    }

    /**
     * 解析FileInfo列表
     */
    private static List<Map<String, Object>> parseFileList(String fileListStr) {
        List<Map<String, Object>> fileList = new ArrayList<>();
        
        // 移除方括号
        String content = fileListStr.substring(1, fileListStr.length() - 1).trim();
        if (content.isEmpty()) {
            return fileList;
        }

        List<String> fileInfoStrings = parseFileInfoItems(content);
        
        for (String fileInfoStr : fileInfoStrings) {
            Map<String, Object> fileInfo = parseFileInfo(fileInfoStr);
            if (!fileInfo.isEmpty()) {
                fileList.add(fileInfo);
            }
        }

        return fileList;
    }

    /**
     * 解析FileInfo项目列表
     */
    private static List<String> parseFileInfoItems(String content) {
        List<String> items = new ArrayList<>();
        StringBuilder currentItem = new StringBuilder();
        int parenthesesCount = 0;
        boolean inQuotes = false;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);

            if (c == '"' && (i == 0 || content.charAt(i - 1) != '\\')) {
                inQuotes = !inQuotes;
                currentItem.append(c);
            } else if (!inQuotes) {
                if (c == '(') {
                    parenthesesCount++;
                    currentItem.append(c);
                } else if (c == ')') {
                    parenthesesCount--;
                    currentItem.append(c);
                } else if (c == ',' && parenthesesCount == 0) {
                    // 找到FileInfo分隔符
                    String item = currentItem.toString().trim();
                    if (!item.isEmpty()) {
                        items.add(item);
                    }
                    currentItem = new StringBuilder();
                } else {
                    currentItem.append(c);
                }
            } else {
                currentItem.append(c);
            }
        }

        // 添加最后一个项目
        if (currentItem.length() > 0) {
            String item = currentItem.toString().trim();
            if (!item.isEmpty()) {
                items.add(item);
            }
        }

        return items;
    }

    /**
     * 解析单个FileInfo对象
     */
    private static Map<String, Object> parseFileInfo(String fileInfoStr) {
        Map<String, Object> fileInfo = new HashMap<>();
        
        // 移除FileInfo()包装
        String content = fileInfoStr.trim();
        if (content.startsWith("FileInfo(") && content.endsWith(")")) {
            content = content.substring(9, content.length() - 1);
        }

        List<String> fields = parseFields(content);
        
        for (String field : fields) {
            if (!isEmptyOrNull(field)) {
                String[] keyValue = parseKeyValue(field);
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();
                    
                    // 尝试转换数值类型
                    if (key.equals("folderId") || key.equals("fileSize")) {
                        try {
                            fileInfo.put(key, Long.parseLong(value));
                        } catch (NumberFormatException e) {
                            fileInfo.put(key, value);
                        }
                    } else {
                        fileInfo.put(key, value.isEmpty() ? null : value);
                    }
                }
            }
        }

        return fileInfo;
    }
}
