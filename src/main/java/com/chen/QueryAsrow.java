package com.chen;

import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class QueryAsrow {//转换为long数组对应的查询
    public static int chunk_num=16;//子索引数量，即文件中为子索引1到子索引8
    private static List<BloomFilter> bloomFilterList;
    private static long[] storage_row;//转置后的按行存储
    private static long query_time_longarray=0;//按行查询，列转行时使用longarray

    public static long getQuery_time_longarray() {
        return query_time_longarray;
    }

    public static void Query(){
        long startqueryASrow=System.currentTimeMillis();
        long total_load_time=0;
        long total_transpose_time=0;
        List<List<String>> all_results=new ArrayList<>();
        String queryfile="D:\\Code\\Idea_Codes\\BIGSI_FILE\\query.txt";
        for(int chunk_index=1;chunk_index<=chunk_num;chunk_index++){

            long startload=System.currentTimeMillis();

            //加载当前子索引的布隆过滤器列表
            String filename="D:/Code/Idea_Codes/BIGSI_FILE/serializeFIle/Col/"+"SubIndex_"+chunk_index+".ser";//序列化子索引的布隆过滤器列表
            bloomFilterList=Utils.deserializeBFlist(filename);
            long start_transpose=System.currentTimeMillis();
            transposeBloomFiltersToLongArray(bloomFilterList);
            long end_transpose=System.currentTimeMillis();
            long transposetime=(end_transpose-start_transpose);
            total_transpose_time+=transposetime;
            bloomFilterList=null;
            // 提示垃圾回收器进行垃圾回收
            System.gc();

            long endload=System.currentTimeMillis();
            long loadTime=endload-startload;//加载当前子索引耗费的时间
            total_load_time+=loadTime;

            List<List<String>> sub_results=new ArrayList<>();
            sub_results=chunk_queryFileASRow(chunk_index,queryfile);

            if (chunk_index==1){//第一个子索引结果
                all_results=sub_results;
            }else {
                for (int i = 0; i < all_results.size(); i++) {
                    all_results.get(i).addAll(sub_results.get(i));
                }
            }

        }

        long endqueryASrow=System.currentTimeMillis();
        long queryTimeASrow=endqueryASrow-startqueryASrow;
        System.out.println("按行查询总时间(转置为long数组):"+queryTimeASrow+"ms");
        System.out.println("加载子索引+转置时间："+total_load_time+"ms");
        System.out.println("转置时间："+total_transpose_time+"ms");
        System.out.println("去除加载子索引和转置后的查询时间:"+(queryTimeASrow-total_load_time)+"ms");

        //结果写入文件
        int pointer = 0;
        String queryresultFile = "D:/Code/Idea_Codes/BIGSI_FILE"+"/"+"query_result(row_longArray).txt";//存放查询结果
        try(
                BufferedReader reader=new BufferedReader(new FileReader(queryfile));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        List<String> cur_result=all_results.get(pointer);
                        if (!cur_result.isEmpty()){//有查询结果
                            writer.write("查询结果"+"\n");
                            for (String datasetName : cur_result) {
                                writer.write(datasetName + "\n");
                            }
                        }else{
                            writer.write("查询结果"+"\n");
                            writer.write("未查询到包含查询序列的数据集"+"\n");
                        }
                        pointer++;
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                List<String> cur_result=all_results.get(pointer);
                if (!cur_result.isEmpty()){//有查询结果
                    writer.write("查询结果"+"\n");
                    for (String datasetName : cur_result) {
                        writer.write(datasetName + "\n");
                    }
                }else{
                    writer.write("查询结果"+"\n");
                    writer.write("未查询到包含查询序列的数据集"+"\n");
                }
            }
        }catch (IOException e){
            System.err.println(e);
        }

    }

    public static void transposeBloomFiltersToLongArray(List<BloomFilter> bloomFilters) {
        long numCols = bloomFilters.size();
        long numRows = Metadata.getBloomfilterSize();

        // 计算所需的long数组的长度，每个long类型可以存储64位数据
        int arrayLength = (int) Math.ceil((double) (numRows * numCols) / Long.SIZE);

        storage_row = new long[arrayLength];

        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                boolean bit = bloomFilters.get(j).getBitSet().get(i);
                if (bit) {
                    // 计算当前位在long数组中的索引和偏移量
                    int index = (int) ((i * numCols + j) / Long.SIZE);
                    int offset = (int) ((i * numCols + j) % Long.SIZE);
                    // 将对应的位设置为1
                    storage_row[index] |= (1L << offset);
                }
            }
        }

    }

    public static List<List<String>> chunk_queryFileASRow(int chunk_index, String filePath){//一个文件中有多个查询长序列，查询每一个并把查询结果写入输出文件
        List<List<String>> sub_results=new ArrayList<>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        sub_results.add(chunk_querySequenceASRow(chunk_index,sequence));
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                //查询最后一段序列
                sub_results.add(chunk_querySequenceASRow(chunk_index,sequence));
            }
        }catch (IOException e){
            System.err.println(e);
        }
//        System.out.println("子索引"+chunk_index+"结果："+sub_results);
        return sub_results;
    }

    public static List<String> chunk_querySequenceASRow(int chunk_index, String sequence) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        long startquery=System.currentTimeMillis();

        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> sub_result=new ArrayList<>(chunk_querykmerASRow(chunk_index,kmerList.get(0)));
        for(String kmer:kmerList){
            sub_result.retainAll(chunk_querykmerASRow(chunk_index,kmer));
        }

        long endquery=System.currentTimeMillis();
        query_time_longarray+=(endquery-startquery);

        if (!sub_result.isEmpty()){
            return sub_result;
        }else {
            return  new ArrayList<>();
        }

    }

    public static List<String> chunk_querykmerASRow(int chunk_index,String kmer){
        int[] indexes = Utils.generateHashes(kmer,Metadata.getNumHashs(),Metadata.getBloomfilterSize());//对应的k个哈希值，即行索引
        List<String> result_sampels=new ArrayList<>();
        List<Integer> dataset_index_list=Metadata.getChunkFileList().get(chunk_index);
        int num_cols=dataset_index_list.size();

        BitSet result=new BitSet(num_cols);
        result.set(0,num_cols);//都设置为1
        BitSet tempbitset=new BitSet(num_cols);

        for(int index:indexes){
            long start_index= (long) index *num_cols;//哈希到的这一行的一个元素的bit索引
            long end_index=start_index+num_cols-1;

            long start_long_index=start_index/Long.SIZE;//开始要取的第一个bit所在的long索引
            long start_bit_index=start_index%Long.SIZE;

            long end_long_index=end_index/Long.SIZE;//要取的最后一个bit所在的long索引
            long end_bit_index=end_index%Long.SIZE;

            //获取start_long_index到end_long_index这（end_long_index-start_long_index+1）个long
            //将start_long_index中的start_bit_index一直到end_long_index中的end_bit_index存入tempbitset中
            // 获取start_long_index到end_long_index这（end_long_index-start_long_index+1）个long
            for (long i = start_long_index; i <= end_long_index; i++) {
                long start_bit = (i == start_long_index) ? start_bit_index : 0;
                long end_bit = (i == end_long_index) ? end_bit_index : Long.SIZE - 1;
//                System.out.println("在"+i+"long中的开始bit："+start_bit);
//                System.out.println("在"+i+"long中的结束bit："+end_bit);

                long mask;
                if (end_bit==63){
                    mask = 0xFFFFFFFFFFFFFFFFL-((1L << start_bit) - 1L);
                }else{
                    mask = (1L << (end_bit + 1)) - (1L << start_bit);
                }
                long value = storage_row[(int) i] & mask;


                // 将value中的bit写入tempbitset中
                for (long j = start_bit; j <= end_bit; j++) {
                    if ((value & (1L << j)) != 0) {
                        tempbitset.set((int) ((i - start_long_index) * Long.SIZE + j - start_bit_index));
                    }
                }
            }

            result.and(tempbitset);
            tempbitset.clear();

        }

        for(int i=0;i<num_cols;i++){
            if (result.get(i)) {
                int dataset_index=dataset_index_list.get(i);
                result_sampels.add(Metadata.getSampleByIndex(dataset_index));
            }
        }

        return result_sampels;
    }



}
