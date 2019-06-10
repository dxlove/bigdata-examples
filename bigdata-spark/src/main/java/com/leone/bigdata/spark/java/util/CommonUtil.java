package com.leone.bigdata.spark.java.util;

import java.io.File;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-24
 **/
public class CommonUtil {

    private CommonUtil() {
    }


    private static SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.ENGLISH);

    private static SimpleDateFormat localTimeSdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");


    /**
     * 获取ip地址的详细信息
     *
     * @param ipAddr
     * @return
     */
    public static String getIpLocation(String ipAddr) {
        return null;
    }

    public static void main(String[] args) {
        String log = "183.238.59.97,,-,,25/Apr/2019:09:52:43 +0800,,GET / HTTP/1.1,,304,,0,,-,,Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36,,-,,-,,1";
        System.out.println(Arrays.toString(log.split(",,")));
        System.out.println(log.split(",,").length);
        System.out.println(getIpLocation("120.197.48.146"));
        System.out.println(dateFormat("25/Apr/2019:09:52:43 +0800"));
    }

    /**
     * @param str
     *
     * @return
     */
    public static String dateFormat(String str) {
        try {
            return localTimeSdf.format(sdf.parse(str));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }


}
