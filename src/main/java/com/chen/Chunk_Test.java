package com.chen;

import org.apache.lucene.search.similarities.IBSimilarity;

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
        //构建
//        int bloomfilterSize= (int) (-1 * (k * b) / Math.log(1 - Math.pow(FPR, 1.0 / k)));
//        System.out.println("布隆过滤器大小:"+bloomfilterSize);
//        long startBuild=System.currentTimeMillis();
//        Index.chunk_BuildIndex(bloomfilterSize,k,inputFiles);
//        long endBuild=System.currentTimeMillis();
//        long BuildTime=(endBuild-startBuild)/ 1000;
//        System.out.println("构建时间"+BuildTime+"秒");
//        //元数据序列化
//        String MetaDataFile="D:/Code/Idea_Codes/BIGSI_FILE/serializeFIle"+"/"+"metadata.ser";
//        Metadata.serialize(MetaDataFile);

        //查询
//        //按列查询
        Metadata.deserialize("D:\\Code\\Idea_Codes\\BIGSI_FILE\\serializeFIle\\metadata.ser");
        Index.chunk_queryAsCol("D:\\Code\\Idea_Codes\\BIGSI_FILE\\query.txt");

//        //进行按行查询之前需要到Index和QueryAsrow里面手动更改子索引数量
//        //按行查询（转置为LongBitset）
        Metadata.deserialize("D:\\Code\\Idea_Codes\\BIGSI_FILE\\serializeFIle\\metadata.ser");
        Index.chunk_queryAsRow("D:\\Code\\Idea_Codes\\BIGSI_FILE\\query.txt");
        System.out.println("直接累加查询时间（去除读写文件）"+Index.getQuery_time_bitset()+"ms");

//        //按行查询（转置为long数组）
        Metadata.deserialize("D:\\Code\\Idea_Codes\\BIGSI_FILE\\serializeFIle\\metadata.ser");
        QueryAsrow.Query();
        System.out.println("直接累加查询时间（去除读写文件）"+QueryAsrow.getQuery_time_longarray()+"ms");






    }



}

