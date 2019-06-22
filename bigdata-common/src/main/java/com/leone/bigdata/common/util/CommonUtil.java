package com.leone.bigdata.common.util;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private static File file;

    static {
        // GeoIP2-City 数据库文件
        file = new File(System.getProperty("user.dir") + File.separator + "GeoLite2-City.mmdb");
    }

    /**
     * 获取ip地址的详细信息
     *
     * @param
     * @return
     */
    public static String getIpLocation(String ipAddr) {
        StringBuilder sb = new StringBuilder();
        try {
            // 创建 DatabaseReader对象
            DatabaseReader reader = new DatabaseReader.Builder(file).build();
            InetAddress ipAddress = InetAddress.getByName(ipAddr);

            CityResponse response = reader.city(ipAddress);
            // 国家
            Country country = response.getCountry();
            sb.append(country.getNames().get("zh-CN")).append(" ");

            // 地区名称
            Subdivision subdivision = response.getMostSpecificSubdivision();
            sb.append(subdivision.getNames().get("zh-CN")).append(" ");

            // 城市名称
            City city = response.getCity();
            sb.append(city.getNames().get("zh-CN")).append(" ");

            // 经纬度
            Location location = response.getLocation();
            sb.append(location.getLatitude()).append(" ").append(location.getLongitude());
            return sb.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static String getIpArea(String ipAddr) {
        StringBuilder sb = new StringBuilder();
        try {
            // 创建 DatabaseReader对象
            DatabaseReader reader = new DatabaseReader.Builder(file).build();
            InetAddress ipAddress = InetAddress.getByName(ipAddr);

            CityResponse city = reader.city(ipAddress);

            // 国家
            Country country = city.getCountry();
            sb.append(country.getNames().get("zh-CN"));

            // 地区名称
            Subdivision subdivision = city.getMostSpecificSubdivision();
            String state = subdivision.getNames().get("zh-CN");
            if (Objects.nonNull(state)) {
                sb.append(" ").append(state);
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    public static void main(String[] args) throws UnknownHostException {
        //String log = "183.238.59.97,,-,,25/Apr/2019:09:52:43 +0800,,GET / HTTP/1.1,,304,,0,,-,,Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36,,-,,-,,1";
        //System.out.println(Arrays.toString(log.split(",,")));
        //System.out.println(log.split(",,").length);
        System.out.println(getIpArea("120.197.48.146"));
        System.out.println(InetAddress.getByName("freedom.2226999.xyz").getHostAddress());
        //System.out.println(dateFormat("25/Apr/2019:09:52:43 +0800"));
    }

    /**
     * 测试网络状态默认超时时间为3秒
     *
     * @param ipAddress
     * @return
     * @throws Exception
     */
    public static boolean ping(String ipAddress) {
        int timeOut = 5000;
        try {
            return InetAddress.getByName(ipAddress).isReachable(timeOut);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * @param str
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

    public static int ping(String ipAddress, int pingCount, int timeOut) {
        BufferedReader in = null;
        Runtime r = Runtime.getRuntime();
        try {
            // 将要执行的ping命令,此命令是windows格式的命令
            Process p = r.exec("ping " + ipAddress + " -n " + pingCount + " -w " + timeOut);
            if (p == null) {
                return 0;
            }
            // 逐行检查输出,计算类似出现=23ms TTL=62字样的次数
            in = new BufferedReader(new InputStreamReader(p.getInputStream(), Charset.forName("GBK")));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                sb.append(line);
            }
            return getTimeOut(sb.toString());
        } catch (Exception ex) {
            ex.printStackTrace();
            return 0;
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取超时时间
     *
     * @param line
     * @return
     */
    private static int getTimeOut(String line) {
        Matcher m2 = Pattern.compile("(?<=平均 = ).*?(?=ms)").matcher(line);
        if (m2.find()) {
            return Integer.valueOf(m2.group());
        }
        return 0;
    }



}
