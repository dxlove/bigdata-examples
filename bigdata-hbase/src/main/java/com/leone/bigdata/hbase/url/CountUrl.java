package com.leone.bigdata.hbase.url;

import com.fasterxml.jackson.core.util.BufferRecycler;
import com.jcraft.jsch.IO;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>找出重复的url
 *
 * @author leone
 * @since 2019-07-13
 **/
public class CountUrl {

    public static void main(String[] args) throws IOException {
        //splitFile();
        foreachFile();
    }

    private static void foreachFile() throws FileNotFoundException {
        List<BufferedReader> bufferedReaderList = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("G:\\" + i + ".log")));
            bufferedReaderList.add(bufferedReader);
        }

        List<String> result = new ArrayList<>();

        HashMap<String, Integer> map = new HashMap<>();
        bufferedReaderList.forEach(e -> {
            try {
                String line;
                while ((line = e.readLine()) != null) {
                    Integer put = map.put(line, 1);
                    if (put != null) {
                        System.out.println(line);
                        result.add(line);
                    }
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }


    private static void splitFile() throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\url.log")));

        List<OutputStream> outputStreamList = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            OutputStream outputStream = new FileOutputStream("G:\\" + i + ".log");
            outputStreamList.add(outputStream);
        }
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            int i = line.hashCode() % 6;
            switch (i) {
                case 0:
                    outputStreamList.get(0).write(line.getBytes());
                    break;
                case 1:
                    outputStreamList.get(1).write(line.getBytes());
                    break;
                case 2:
                    outputStreamList.get(2).write(line.getBytes());
                    break;
                case 3:
                    outputStreamList.get(3).write(line.getBytes());
                    break;
                case 4:
                    outputStreamList.get(4).write(line.getBytes());
                    break;
                case 5:
                    outputStreamList.get(5).write(line.getBytes());
                    break;
                default:
                    break;
            }

        }

        // 关闭流
        outputStreamList.forEach(e -> {
            try {
                e.flush();
                e.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        });
    }


}
