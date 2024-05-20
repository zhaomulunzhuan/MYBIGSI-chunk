package com.chen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ComputeMaxCardinality {//由于构建BIGSI需要通过数据集最大基数确定布隆过滤器大小，我们的工作中预处理阶段获得了数据集基数，用来借助找到最大基数
    public static void main(String[] args) {
        // 文件路径
        String filePath = "D:\\SequenceSearch_2\\dataidx_to_cardinality.txt";

        // 初始化最大基数为0
        int maxBase = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            // 逐行读取文件内容
            while ((line = br.readLine()) != null) {
                // 拆分每行的数据，以逗号分隔
                String[] parts = line.split(",");
                int base = Integer.parseInt(parts[1]);
                // 如果当前基数大于最大基数，则更新最大基数
                if (base > maxBase) {
                    maxBase = base;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 打印最大基数
        System.out.println("最大基数是：" + maxBase);
    }
}
