package com.troy.util;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * 获取properties文件中的值
 */
public class PropertiesUtil {
    Properties pro;

    /**
     * 创建对象
     *
     * @param fileName 传入需要加载的文件名
     */
    public PropertiesUtil(String fileName) {
        pro = new Properties();
        InputStream is = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
        InputStreamReader isr = null;
        try {
            isr = new InputStreamReader(is, "utf-8");
            pro.load(isr);
            isr.close();
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 传入某个key，得到对应的值
     *
     * @param key 传入key的值
     * @return 返回对应的值，如果没有该key，返回null
     */
    public String getPropertie(String key) {
        return pro.getProperty(key);
    }
}
