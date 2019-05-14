package com.troy.util;

import java.util.Random;

/**
 * 随机数工具类
 */
public class RandomUtil {
    /**
     * 生成0~num-1的整型数据
     * 如：传10,返回0~9的随机数
     * @param num
     * @return
     */
    public static int getZeroToNum(int num){
        return new Random().nextInt(num);
    }
}
