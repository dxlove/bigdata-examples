package com.leone.bigdata.common.test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leone.bigdata.common.entity.Http;
import com.leone.bigdata.common.util.CommonUtil;
import com.leone.bigdata.common.util.DBUtil;
import com.leone.bigdata.common.util.RegexUtil;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-26
 **/
public class ResourceEdit {

    private static DBUtil.Config config = new DBUtil.Config(null, null, null, null, null);
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        List<String> select = DBUtil.getInstance().select(config, "select * from t_http", Http.class);
        select.forEach(e -> {
            try {
                System.out.println(e);
                Http ss = objectMapper.readValue(e, new TypeReference<Http>() {
                });
                if (RegexUtil.checkIp(ss.getIp())) {
                    int ping = CommonUtil.ping(ss.getIp(), 4, 4000);
                    // 设置超时时间
                    String sql = "update t_http set timeout = " + ping + " where http_id = " + ss.getHttp_id();
                    DBUtil.execute(config, sql);
                    // 设置是否可用
                    if (ping != 0) {
                        DBUtil.execute(config, "update t_http set enable = 1 where http_id = " + ss.getHttp_id());
                        System.out.println(sql);
                    } else {
                        DBUtil.execute(config, "update t_http set enable = 0 where http_id = " + ss.getHttp_id());
                        System.err.println(sql);
                    }
                    // 设置国家信息
                    String country = CommonUtil.getIpArea(ss.getIp());
                    DBUtil.execute(config, "update t_http set country = '" + country + "' where http_id = " + ss.getHttp_id());
                } else {
                    //System.out.println(ssr.getIp());
                    //String address = InetAddress.getByName(ssr.getIp()).getHostAddress();
                    //String sql = "update t_ssr set ip = " + address + " where ssr_id = " + ssr.getSsr_id();
                    //execute("ip", 3306, "db02", "xxx", "xxx", sql);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }

}
