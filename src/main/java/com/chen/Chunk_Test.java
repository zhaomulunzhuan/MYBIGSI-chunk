package com.chen;

import java.io.IOException;
import java.util.List;

public class Chunk_Test {//测试分批构建 分批按列查询 分批按行查询
    public static void main(String[] args) throws IOException {
        //每一行是一个kmer数据集的地址
        String filePath = "D:\\Code\\Idea_Codes\\BIGSI_FILE\\BIGSI_inputfiles.txt";

        List<String> inputFiles = Utils.readInputFiles(filePath);
        int b=12822947;//最大基数
        float FPR= 0.01F;//误报率
        int k=7;
        int bloomfilterSize= (int) (-1 * (k * b) / Math.log(1 - Math.pow(FPR, 1.0 / k)));
        long startBuild=System.currentTimeMillis();

        Index.chunk_BuildIndex(bloomfilterSize,k,inputFiles);

        long endBuild=System.currentTimeMillis();
        long BuildTime=(endBuild-startBuild)/ 1000;

        System.out.println("构建时间"+BuildTime+"秒");
        //元数据序列化
        String MetaDataFile="D:/Code/Idea_Codes/BIGSI_FILE/serializeFIle"+"/"+"metadata.ser";
        Metadata.serialize(MetaDataFile);

        //按列查询 按行查询直接在构建时执行，没有序列化下来，按列构建的索引序列化下来了
//        Metadata.deserialize("D:\\Code\\Idea_Codes\\BIGSI_FILE\\serializeFIle\\metadata.ser");
//        Index.chunk_queryAsCol("D:\\Code\\Idea_Codes\\BIGSI_FILE\\query.txt");



    }



}

