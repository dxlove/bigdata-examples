package com.leone.bigdata.common.util;

import com.leone.bigdata.common.entity.Order;
import com.leone.bigdata.common.entity.User;

import java.util.*;
import java.util.stream.Collectors;

/**
 * <p>
 *
 * @author leone
 * @since 2018-06-26
 **/
public class EntityFactory {

    private static Random random = new Random();

    private static List<User> userList = new LinkedList<>();

    private static List<Order> orderList = new LinkedList<>();

    static {
        for (long i = 0; i < 100; i++) {
            Date date = RandomValue.randomDateTime();
            userList.add(new User(i, RandomValue.randomUsername(), RandomValue.randomStr(16), RandomValue.randomWords(), RandomValue.RANDOM.nextInt(50) + 10, date, false));
            orderList.add(new Order(i, i, random.nextInt(1000) + 200, "Chicken and fish", 1 + RandomValue.randomNum(15), date, date, false));
            orderList.add(new Order(i, i, random.nextInt(100) + 200, "some apple and orange", 1 + RandomValue.randomNum(15), date, date, false));
        }
    }

    /**
     * 获取object数据格式数据
     *
     * @param count
     * @return
     */
    public static List<User> getUsers(Integer count) {
        List<User> userList = new ArrayList<>();
        if (count < 1) {
            return Collections.emptyList();
        }
        for (long i = 0; i < count; i++) {
            userList.add(new User(i, RandomValue.randomUsername(), RandomValue.randomStr(16), RandomValue.randomWords(), RandomValue.RANDOM.nextInt(50) + 10, new Date(), false));
        }
        return userList;
    }


    /**
     * @return
     */
    public static User getUser() {
        return new User(0L, RandomValue.randomUsername(), RandomValue.randomStr(16), RandomValue.randomWords(), RandomValue.RANDOM.nextInt(50) + 10, new Date(), false);
    }

    /**
     * @param userId
     * @return
     */
    public static User getUser(Long userId) {
        return userList.stream().filter(e -> e.getUserId().equals(userId)).collect(Collectors.toList()).get(0);
    }

    /**
     * @param userId
     */
    public static void remove(Long userId) {
        userList.removeIf(next -> next.getUserId().equals(userId));
    }

    /**
     *
     * @param userId
     * @return
     */
    public static List<Order> getOrderList(Long userId) {
        return orderList.stream().filter(e -> e.getUserId().equals(userId)).collect(Collectors.toList());
    }

    /**
     *
     * @param orderId
     * @return
     */
    public static Order getOrder(Long orderId) {
        return orderList.stream().filter(e -> e.getUserId().equals(orderId)).collect(Collectors.toList()).get(0);
    }

}