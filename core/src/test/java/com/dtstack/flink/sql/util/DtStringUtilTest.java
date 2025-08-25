package com.dtstack.flink.sql.util;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class DtStringUtilTest {

    @Test
    public void testSplitIgnoreQuota(){
        DtStringUtil.splitIgnoreQuota("(ss, aa)", ',');
    }

    @Test
    public void testReplaceIgnoreQuota(){
        String str = DtStringUtil.replaceIgnoreQuota("abcdfs", "s", "a");
        Assert.assertEquals(str, "abcdfa");

    }

    @Test
    public void testCol2string(){
        Assert.assertEquals(DtStringUtil.col2string(1, "TINYINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "SMALLINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "INT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "BIGINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "FLOAT"), Float.valueOf(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "DOUBLE"), Double.valueOf(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "DECIMAL"), new BigDecimal(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "CHAR"), "1");
        Assert.assertEquals(DtStringUtil.col2string(true, "BOOLEAN"), Boolean.valueOf(true).toString());
        Assert.assertEquals(DtStringUtil.col2string(new Date(), "DATE"), DateUtil.dateToString(new Date()));
        Assert.assertEquals(DtStringUtil.col2string(new Date(), "TIMESTAMP"), DateUtil.timestampToString(new Date()));
        try {
            Assert.assertEquals(DtStringUtil.col2string(1, "other"), "1");
        } catch (Exception e){

        }
    }

    @Test
    public void testGetPluginTypeWithoutVersion(){
        Assert.assertEquals(DtStringUtil.getPluginTypeWithoutVersion("kafka10"), "kafka");
    }

    @Test
    public void testAddJdbcParam(){
        try {
            DtStringUtil.addJdbcParam(null, null, false);
        }catch (Exception e){

        }
        DtStringUtil.addJdbcParam("ss", null, false);
        Map<String, String> addParams = Maps.newHashMap();
        addParams.put("aa", "bb");
        DtStringUtil.addJdbcParam("jdbc:mysql://172.16.8.104:3306/test?charset=utf8", addParams, false);
    }

    @Test
    public void testIsJson(){
        Assert.assertEquals(DtStringUtil.isJson("{}"), true);
        try {
            DtStringUtil.isJson("");
        }catch (Exception e){

        }
    }

    @Test
    public void testParse(){
        Assert.assertEquals(DtStringUtil.parse("1", Integer.class), 1);
        Assert.assertEquals(DtStringUtil.parse("1", Long.class), Long.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("1", Byte.class), "1".getBytes()[0]);
        Assert.assertEquals(DtStringUtil.parse("1", String.class), "1");
        Assert.assertEquals(DtStringUtil.parse("1", Float.class), Float.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("1", Double.class), Double.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("2020-02-03 11:33:33", Timestamp.class), Timestamp.valueOf("2020-02-03 11:33:33"));
        try {
            Assert.assertEquals(DtStringUtil.parse("1", Object.class), 1);
        } catch (Exception e){

        }
    }

    @Test
    public void testFirstUpperCase(){
        Assert.assertEquals(DtStringUtil.firstUpperCase("abc"), "Abc");;
    }

    @Test
    public void testGetTableFullPath(){
        Assert.assertEquals(DtStringUtil.getTableFullPath("aa", "aa.roc"), "\"aa\".\"roc\"");
    }

    @Test
    public void testConvertDataStructureToJson() {
        // 测试空输入
        Assert.assertEquals(DtStringUtil.convertDataStructureToJson(""), "{}");
        Assert.assertEquals(DtStringUtil.convertDataStructureToJson(null), "{}");

        // 测试简单对象转换
        String simpleInput = "(name=test, value=123, empty=)";
        String simpleJson = DtStringUtil.convertDataStructureToJson(simpleInput);
        Assert.assertTrue(DtStringUtil.isJson(simpleJson));
        Assert.assertTrue(simpleJson.contains("\"name\":\"test\""));
        Assert.assertTrue(simpleJson.contains("\"value\":\"123\""));
        Assert.assertTrue(simpleJson.contains("\"empty\":null"));

        // 测试完整的FileInfo结构
        String complexInput = "(uniscid=91110115WNRW1W8CG7, ywid=212028d73e404450884f3cb720184747, " +
                "firstRegAcceNumber=测试数据受理号, productName=test优先数据, " +
                "applicantName=北京一证通测试有限二公司, priorityReason=产品原因申请优先审批, " +
                "remark=, contacts=张欢欢, contactPhoneNumber=13123212321, " +
                "fileList=[FileInfo(fileName=379b4827-b102-4e97-87e3-3ae492b11db8.pdf, " +
                "folderId=4692518625788739592, fileId=3d1fb4db5a9b40a2a15d3587aaff2226, " +
                "fileType=0302_01, fileSize=84601), " +
                "FileInfo(fileName=379b4827-b102-4e97-87e3-3ae492b11db8.pdf, " +
                "folderId=4692518625788739592, fileId=a3f2017f67784e2e8d3099da358fbb3c, " +
                "fileType=0302_02, fileSize=84601), " +
                "FileInfo(fileName=379b4827-b102-4e97-87e3-3ae492b11db8.pdf, " +
                "folderId=4692518625788739592, fileId=f028f706e3dd4e47bed03a23cc14e5fe, " +
                "fileType=0302_06, fileSize=84601)], mainMechanism=test优先数据)";

        String complexJson = DtStringUtil.convertDataStructureToJson(complexInput);
        Assert.assertTrue(DtStringUtil.isJson(complexJson));
        
        // 验证主要字段存在
        Assert.assertTrue(complexJson.contains("\"uniscid\":\"91110115WNRW1W8CG7\""));
        Assert.assertTrue(complexJson.contains("\"ywid\":\"212028d73e404450884f3cb720184747\""));
        Assert.assertTrue(complexJson.contains("\"firstRegAcceNumber\":\"测试数据受理号\""));
        Assert.assertTrue(complexJson.contains("\"productName\":\"test优先数据\""));
        Assert.assertTrue(complexJson.contains("\"applicantName\":\"北京一证通测试有限二公司\""));
        Assert.assertTrue(complexJson.contains("\"priorityReason\":\"产品原因申请优先审批\""));
        Assert.assertTrue(complexJson.contains("\"remark\":null"));
        Assert.assertTrue(complexJson.contains("\"contacts\":\"张欢欢\""));
        Assert.assertTrue(complexJson.contains("\"contactPhoneNumber\":\"13123212321\""));
        Assert.assertTrue(complexJson.contains("\"mainMechanism\":\"test优先数据\""));
        
        // 验证fileList数组存在
        Assert.assertTrue(complexJson.contains("\"fileList\":["));
        Assert.assertTrue(complexJson.contains("\"fileName\":\"379b4827-b102-4e97-87e3-3ae492b11db8.pdf\""));
        Assert.assertTrue(complexJson.contains("\"folderId\":4692518625788739592"));
        Assert.assertTrue(complexJson.contains("\"fileSize\":84601"));
        Assert.assertTrue(complexJson.contains("\"fileType\":\"0302_01\""));
        Assert.assertTrue(complexJson.contains("\"fileType\":\"0302_02\""));
        Assert.assertTrue(complexJson.contains("\"fileType\":\"0302_06\""));

        // 测试空的fileList
        String emptyFileListInput = "(name=test, fileList=[], value=123)";
        String emptyFileListJson = DtStringUtil.convertDataStructureToJson(emptyFileListInput);
        Assert.assertTrue(DtStringUtil.isJson(emptyFileListJson));
        Assert.assertTrue(emptyFileListJson.contains("\"fileList\":[]"));
    }


}
