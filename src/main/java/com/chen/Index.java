package com.chen;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.util.LongBitSet;
public class Index implements Serializable{
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private static List<BloomFilter> bloomFilterList;
    private static LongBitSet longBitSet;//由于每行转置成一个bitset，即使用List<BitSet>，总是造成堆内存溢出，所以布隆过滤器列表转置为一个位数组
    private static List<List<String>> all_results=new ArrayList<>();//按行查询结果
    private static int chunk_num=16;//子索引数量
    private static long query_col=0;

    private static long query_time_bitset=0;//按行查询，列转行时使用bitset

    public static long getQuery_time_bitset() {
        return query_time_bitset;
    }
    public static long getQuery_col(){
        return query_col;
    }

    //分批构建
    public static void chunk_BuildIndex(int bloomfilter_size, int k, List<String> input_file){//分批构建 为了便于行查询，可以128一批，或者256一批
        Metadata.set_bloomfilter_size(bloomfilter_size);
        Metadata.set_num_hashs(k);
        int chunkSize=128;//每128个数据集构建一个BIGSI索引
        processChunks(bloomfilter_size, k,input_file,chunkSize);
    }

    public static void processChunks(int bloomfilter_size, int k,List<String> inputList, int chunkSize) {
        Stream<List<String>> chunksStream = chunkStream(inputList, chunkSize);
        AtomicInteger batchIndex = new AtomicInteger(1); // Counter for batch index

        // Iterate over each chunk lazily
        chunksStream.forEach(chunk -> {//分别处理每个子列表
            //当前批号
            int currentIndex = batchIndex.getAndIncrement();
            // 每个chunk是一个长度小于等于chunk_size的数据数据集路径子列表
            Build_child_Index(currentIndex,bloomfilter_size,k,chunk);

        });
    }

    public static Stream<List<String>> chunkStream(List<String> inputList, int chunkSize) {
        int size = inputList.size();
        int numOfChunks = (size + chunkSize - 1) / chunkSize; // Calculate the number of chunks

        return IntStream.range(0, numOfChunks).mapToObj(
                i -> inputList.subList(i * chunkSize, Math.min((i + 1) * chunkSize, size))
        );
    }

    public static void Build_child_Index(int chunk_index, int bloomfilter_size, int k, List<String> sub_input_file){//按批次构建子列表
//        System.out.println("构建批次"+chunk_index+"，数据集列表："+sub_input_file);
        bloomFilterList=new ArrayList<>();
//        bitSetList=new ArrayList<>();
        List<Integer> dataset_index_list=new ArrayList<>();
        for(int i=0;i< sub_input_file.size();i++){
            //获取文件名，不包含扩展名
            Path inputPath = Paths.get(sub_input_file.get(i));
            // 获取文件名（包含扩展名）
            String fileNameWithExtension = inputPath.getFileName().toString();
            // 获取不包含扩展名的文件名
            String fileName = fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'));
            if (Metadata.sampleToColour.containsKey(fileName)){
                System.err.println(fileName + " already in BIGSI index!");
            }else {
                Metadata.add_sample(fileName);
                int dataset_index=Metadata.getIndexBySample(fileName);//数据集索引
                dataset_index_list.add(dataset_index);
                //一次把一个文件内的kmer都读取到内存
//                List<String> kmers=Utils.getKmers(String.valueOf(inputPath));//得到输入文件的kmers列表
//                for(String kmer:kmers){
//                    bloomFilter.add(kmer);
//                }
                //构建这个批次对应的布隆过滤器列表
                BloomFilter bloomFilter=new BloomFilter(bloomfilter_size,k);

                //一个个从文件中读取kmer然后添加 看看能否解决超出内存的问题
                try (BufferedReader reader=new BufferedReader(new FileReader(String.valueOf(inputPath)))){
                    String kmer;
                    while ((kmer=reader.readLine())!=null){
                        bloomFilter.add(kmer);
                    }
                }catch (IOException e){
                    e.printStackTrace();
                }

                bloomFilterList.add(bloomFilter);//创建一个布隆过滤器
            }
        }
        Metadata.getChunkFileList().put(chunk_index,dataset_index_list);

        //=======================================最终获得按列构建的索引
        String filename="D:/Code/Idea_Codes/BIGSI_FILE/serializeFIle/Col/"+"SubIndex_"+chunk_index+".ser";//序列化子索引的布隆过滤器列表
        Utils.serializeBFlist(bloomFilterList,filename);


    }

    //按列查询
    public static void chunk_queryAsCol(String queryfile){//应该是加载一次子索引对查询文件执行一次查询
        long startqueryAScol=System.currentTimeMillis();
        long total_load_time=0;
        String directoryPath = "D:\\Code\\Idea_Codes\\BIGSI_FILE\\serializeFIle\\Col";
        File directory = new File(directoryPath);
        List<List<String>> all_results=new ArrayList<>();
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    List<List<String>> sub_results=new ArrayList<>();
                    if (file.isFile()) {//一个file对应一个子索引的布隆过来器列表
                        String fileName = file.getName();
                        //子索引序列化文件名 如 SubIndex_1.ser 所示
                        String[] parts = fileName.split("_");
                        if (parts.length >= 2) {
                            String numberStr = parts[1].replaceAll("\\.ser$", "");
                            int index = Integer.parseInt(numberStr);//批次号,从1开始
//                            System.out.println("查询子索引: " + index + " in file: " + fileName);
                            //获得它对应的数据集索引列表
                            List<Integer> dataset_index_list=Metadata.getChunkFileList().get(index);
                            // 处理当前子索引
                            //记录加载时间
                            long startload=System.currentTimeMillis();
                            //加载当前子索引的布隆过滤器列表
                            bloomFilterList=Utils.deserializeBFlist(file.getPath());
                            long endload=System.currentTimeMillis();
                            long loadTime=endload-startload;//加载当前子索引耗费的时间
                            total_load_time+=loadTime;

                            long startquery=System.currentTimeMillis();
                            //对当前子索引执行文件查询
                            sub_results=chunk_queryFileAScol(index,queryfile);
                            if (index==1){//第一个子索引结果
                                all_results=sub_results;
                            }else{
                                for(int i=0;i<all_results.size();i++){
                                    all_results.get(i).addAll(sub_results.get(i));
                                }
                            }
                            long endquery=System.currentTimeMillis();
                            query_col+=(endquery-startquery);
                        }
                    }
                }
//                System.out.println(all_results);
                //结果写入文件
                int pointer = 0;
                String queryresultFile = "D:/Code/Idea_Codes/BIGSI_FILE"+"/"+"query_result(col).txt";//存放查询结果
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
        } else {
            System.out.println("Invalid directory path: " + directoryPath);
        }

        long endqueryAScol=System.currentTimeMillis();
        long queryTimeAScol=endqueryAScol-startqueryAScol;
        System.out.println("按列查询总时间(包括加载子索引):"+queryTimeAScol+"ms");
        System.out.println("加载子索引时间"+total_load_time+"ms");
        System.out.println("去除加载子索引时间:"+(queryTimeAScol-total_load_time)+"ms");
    }

    public static List<String> chunk_querykmerAScol(int chunk_index,String kmer){//在批号为chunk_index的子索引中查询kmer
        int[] indexes = Utils.generateHashes(kmer,Metadata.getNumHashs(),Metadata.getBloomfilterSize());//对应的k个哈希值，即行索引
        List<String> result_sampels=new ArrayList<>();
        List<Integer> dataset_index_list=Metadata.getChunkFileList().get(chunk_index);
        for(int i=0;i<bloomFilterList.size();i++){
            BloomFilter bloomFilter=bloomFilterList.get(i);
            if(bloomFilter.test(indexes)){
                int dataset_index=dataset_index_list.get(i);
                result_sampels.add(Metadata.getSampleByIndex(dataset_index));
            }
        }
        return result_sampels;
    }

    public static List<String> chunk_querySequenceAScol(int chunk_index, String sequence) throws IOException {//查找长序列，每个kmer都存在才报告序列存在
        int kmersize=31;//根据数据集kmer长度简单写死
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> sub_result=new ArrayList<>(chunk_querykmerAScol(chunk_index,kmerList.get(0)));
        for(String kmer:kmerList){
            sub_result.retainAll(chunk_querykmerAScol(chunk_index,kmer));
        }

        // 将查询结果写入到结果文件
        if (!sub_result.isEmpty()){
            return sub_result;
        }else {
            return  new ArrayList<>();
        }

    }

    public static List<List<String>> chunk_queryFileAScol(int chunk_index, String filePath){//一个文件中有多个查询长序列，查询每一个并把查询结果写入输出文件
        List<List<String>> sub_results=new ArrayList<>();
        try(BufferedReader reader=new BufferedReader(new FileReader(filePath))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        sub_results.add(chunk_querySequenceAScol(chunk_index,sequence));
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                //查询最后一段序列
                sub_results.add(chunk_querySequenceAScol(chunk_index,sequence));
            }
        }catch (IOException e){
            System.err.println(e);
        }
        return sub_results;
    }


    //分批 按行 查询  这里的按行查询是将布隆过滤器列表转换为一个LongBitset，而QueryAsrow里面是将其转换为long数组
    //按行查询
    public static void chunk_queryAsRow(String queryfile){
        long startqueryASrow=System.currentTimeMillis();
        long total_load_time=0;
        long total_transpose_time=0;
        List<List<String>> all_results=new ArrayList<>();
        for(int chunk_index=1;chunk_index<=chunk_num;chunk_index++){

            long startload=System.currentTimeMillis();

            //加载当前子索引的布隆过滤器列表
            String filename="D:/Code/Idea_Codes/BIGSI_FILE/serializeFIle/Col/"+"SubIndex_"+chunk_index+".ser";//序列化子索引的布隆过滤器列表
            bloomFilterList=Utils.deserializeBFlist(filename);
            long start_transpose=System.currentTimeMillis();
            transposeBloomFiltersToLongBitset(bloomFilterList);
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
            sub_results=Index.chunk_queryFileASRow(chunk_index,queryfile);

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
        System.out.println("按行查询总时间(转置为LongBitset):"+queryTimeASrow+"ms");
        System.out.println("加载子索引+转置时间："+total_load_time+"ms");
        System.out.println("转置时间："+total_transpose_time+"ms");
        System.out.println("去除加载子索引和转置后的查询时间:"+(queryTimeASrow-total_load_time)+"ms");

        //结果写入文件
        int pointer = 0;
        String queryresultFile = "D:/Code/Idea_Codes/BIGSI_FILE"+"/"+"query_result(row_LongBitset).txt";//存放查询结果
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

    public static void transposeBloomFiltersToLongBitset(List<BloomFilter> bloomFilters) {//如果一个布隆过滤器是一列，这里就是按列存储转换为按行存储
        int numCols=bloomFilters.size();
        int numRows=Metadata.getBloomfilterSize();
        longBitSet=new LongBitSet((long) numRows *numCols);
        for(int i=0;i<numRows;i++){
            for(int j=0;j<numCols;j++){
                boolean bit=bloomFilters.get(j).getBitSet().get(i);
                if (bit){
                    longBitSet.set((long) i *numCols+j);
                }
            }
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
            long start_index= (long) index *num_cols;
            long end_index=start_index+num_cols;
            //清空临时bitset
            tempbitset.clear();
            //复制指定范围的位
            for(long i=start_index;i<end_index;i++){
                tempbitset.set((int) (i-start_index),longBitSet.get(i));
            }
            result.and(tempbitset);
        }
        for(int i=0;i<num_cols;i++){
            if (result.get(i)) {
                int dataset_index=dataset_index_list.get(i);
                result_sampels.add(Metadata.getSampleByIndex(dataset_index));
            }
        }



        return result_sampels;
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
        query_time_bitset+=(endquery-startquery);

        // 将查询结果写入到结果文件
        if (!sub_result.isEmpty()){
            return sub_result;
        }else {
            return  new ArrayList<>();
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



}
