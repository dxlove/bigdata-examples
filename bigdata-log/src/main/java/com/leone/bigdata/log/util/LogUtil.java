package com.leone.bigdata.log.util;

import com.leone.bigdata.common.util.RandomValue;

import java.io.*;
import java.time.LocalDateTime;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-25
 **/
public abstract class LogUtil {


    public static void main(String[] args) throws IOException {
        //OutputStream outputStream = new FileOutputStream("D:\\url.log.bak");
        //long start = System.currentTimeMillis();
        //for (int i = 0; i < Integer.MAX_VALUE; i++) {
        //    outputStream.write((RandomValue.randomUrl() + "\r\n").getBytes());
        //    if (i % 10000000 == 0) {
        //        System.out.println(LocalDateTime.now());
        //    }
        //}
        //System.out.println(System.currentTimeMillis() - start);
        String a = "http://abcd.com";
        String b = "http://abcd.com";
        System.out.println(a.hashCode());
        System.out.println(b.hashCode());
    }

}
