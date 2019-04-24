package com.leone.spark.utils;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;

import java.io.File;
import java.net.InetAddress;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-24
 **/
public class CommonUtil {

    private CommonUtil() {
    }

    /**
     * 获取ip地址的详细信息
     *
     * @param ipAddr
     * @return
     */
    public static String getIpLocation(String ipAddr) {
        StringBuilder sb = new StringBuilder();
        try {
            // GeoIP2-City 数据库文件
            File file = new File(System.getProperty("user.dir") + File.separator + "GeoLite2-City.mmdb");

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

    public static void main(String[] args) {
        System.out.println(getIpLocation("120.197.48.146"));
    }


}
