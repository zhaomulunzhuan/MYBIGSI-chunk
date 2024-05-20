package com.chen;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Metadata implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private static final String DELETION_SPECIAL_SAMPLE_NAME = "DELETION_SPECIAL_SAMPLE_NAME";
    private static int colour_count;//数据集数量
    static Map<String, Integer> sampleToColour;//数据集名称到数据集索引
    private static List<String> colourToSample;//数据集索引到数据集名称
    private static Map<Integer, List<Integer>> chunk_file_list;//每个List<String>是一个批次对应的数据集索引列表
    private static int nums_rows;//行数
    private static int num_cols;//列数
    private static int bloomfilterSize;//布隆过滤器大小
    private static int numHashs;//哈希函数数量

    static {
        colour_count=0;
        sampleToColour=new HashMap<>();
        colourToSample=new ArrayList<>();
        chunk_file_list=new HashMap<Integer, List<Integer>>();
    }

    public static Map<Integer, List<Integer>> getChunkFileList() {
        return chunk_file_list;
    }
    public static void set_bloomfilter_size(int bloomfilter_size){
        bloomfilterSize=bloomfilter_size;
    }

    public static int getBloomfilterSize(){
        return bloomfilterSize;
    }

    public static void set_num_hashs(int num_hashs){
        numHashs=num_hashs;
    }

    public static int getNumHashs(){
        return numHashs;
    }

    public static int num_samples(){
        return colour_count;
    }

    public static int getNums_rows(){
        return nums_rows;
    }

    public void setNums_rows(int nums){
        nums_rows=nums;
    }

    public static int getNum_cols(){
        return num_cols;
    }

    public void setNum_cols(int nums){
        num_cols=nums;
    }

    public static Map<String, Integer> getSampleToColour() {
        return sampleToColour;
    }

    public static List<String> getColourToSample() {
        return colourToSample;
    }

    public static void add_sample(String sample_name){//添加数据集
        validateSampleName(sample_name);//检查样本名称
        int cur_index=colour_count;
        sampleToColour.put(sample_name,cur_index);
        colourToSample.add(sample_name);
        colour_count++;
    }

    public void add_samples(List<String> sample_names){
        for(String sample_name:sample_names){
            add_sample(sample_name);
        }
    }

    public static Integer getIndexBySample(String sampleName) {//数据集名获得数据集索引
        if (sampleToColour.containsKey(sampleName)) {
            return sampleToColour.get(sampleName);
        } else {
            return null;
        }
    }

    public static String getSampleByIndex(int colour) {//数据集索引获得数据集名
        if (colour>=0 && colour<colourToSample.size()) {
            return colourToSample.get(colour);
        } else {
            return null;
        }
    }

    public Map<String, Integer> samplesToColours(List<String> sampleNames) {//获得一个数据集列表的对应的数据集索引表，即键是数据集名称，值是对应索引
        Map<String, Integer> result = new HashMap<>();
        for (String sampleName : sampleNames) {
            Integer colour = getIndexBySample(sampleName);
            if (colour != null) {
                result.put(sampleName, colour);
            }
        }
        return result;
    }

    public Map<Integer, String> coloursToSamples(List<Integer> colours) {//接受一个颜色（即索引）列表作为输入，并返回一个映射，其中键是颜色（即索引），值是对应的样本名称。如果颜色不存在或已删除，则不包含在结果映射中。
        Map<Integer, String> result = new HashMap<>();
        for (int colour : colours) {
            String sampleName = getSampleByIndex(colour);
            if (sampleName != null) {
                result.put(colour, sampleName);
            }
        }
        return result;
    }

    public static void validateSampleName(String sampleName) throws IllegalArgumentException {//查看数据集名称是否有效
        if (sampleName.equals(DELETION_SPECIAL_SAMPLE_NAME)) {
            throw new IllegalArgumentException("You can't call a sample " + DELETION_SPECIAL_SAMPLE_NAME);
        }
        // 检查样本名称是否已存在
        if (sampleToColour.containsKey(sampleName)) {
            throw new IllegalArgumentException("You can't insert two samples with the same name");
        }
    }

    // 序列化成员函数
    public static void serialize(String filePath) {//元数据序列化
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeInt(colour_count);
            oos.writeObject(sampleToColour);
            oos.writeObject(colourToSample);
            oos.writeObject(chunk_file_list);
            oos.writeInt(nums_rows);
            oos.writeInt(num_cols);
            oos.writeInt(bloomfilterSize);
            oos.writeInt(numHashs);
            System.out.println("Metadata 对象已成功序列化到 " + filePath + " 文件中");
        } catch (IOException e) {
            System.err.println("序列化失败：" + e.getMessage());
        }
    }

    // 反序列化成员函数
    public static void deserialize(String filePath) {//反序列化，即从文件中加载Metadata类的各成员信息
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            colour_count = ois.readInt();
            sampleToColour = (Map<String, Integer>) ois.readObject();
            colourToSample = (List<String>) ois.readObject();
            chunk_file_list= (Map<Integer, List<Integer>>) ois.readObject();
            nums_rows = ois.readInt();
            num_cols = ois.readInt();
            bloomfilterSize = ois.readInt();
            numHashs = ois.readInt();
            System.out.println("从 " + filePath + " 文件中成功反序列化了 Metadata 对象");
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("反序列化失败：" + e.getMessage());
        }
    }

    public static void printMetadata() {
        System.out.println("colour_count: " + colour_count);
        System.out.println("sampleToColour: " + sampleToColour);
        System.out.println("colourToSample: " + colourToSample);
        System.out.println("nums_rows: " + nums_rows);
        System.out.println("num_cols: " + num_cols);
        System.out.println("bloomfilterSize: " + bloomfilterSize);
        System.out.println("numHashs: " + numHashs);
        System.out.println("各个批次信息");
        for (Map.Entry<Integer, List<Integer>> entry : Metadata.getChunkFileList().entrySet()) {
            int chunkIndex = entry.getKey();
            List<Integer> fileList = entry.getValue();
            System.out.println("Chunk " + chunkIndex + ": " + fileList);
        }
    }
}
