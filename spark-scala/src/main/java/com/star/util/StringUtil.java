package com.star.util;

import java.io.Serializable;
import java.security.MessageDigest;

/**
 * Created by jinwei on 17-11-17.
 */
public class StringUtil implements Serializable {
    public static String md5(String s) {
        char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void main(String[] args) {
        IDUtil util = new IDUtil();
        long begin = System.currentTimeMillis();
        for(int i=0;i<1000000;i++){
            StringUtil.md5("测试"+"510183801109008"+"MRct8RVFmCqEHxRUL2yjqJ73a2ExSbW8");
//            char sign = util.calcTrailingNumber("51018319801109008".toCharArray());
        }
        long end = System.currentTimeMillis();
        System.out.println((end-begin)/1000.0+" s");
    }
}
