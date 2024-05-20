package com.chen;

import java.io.Serializable;
import java.util.BitSet;
import java.util.List;

public class BloomFilter implements Serializable {
    // 实现 Serializable 接口
    private static final long serialVersionUID = 1L;
    private int m; // 布隆过滤器的位数
    private int h; // 哈希函数个数
    private BitSet bitSet; // 位数组

    public BloomFilter(int m, int h) {
        this.m = m;
        this.h = h;
        this.bitSet = new BitSet(m);
    }

    public BitSet getBitSet(){
        return bitSet;
    }

    public int get_bloomfilter_size(){
        return m;
    }

    public void set_bloomfilter_size(int bloomfilterSize){
        m=bloomfilterSize;
    }

    public int get_num_hashs(){
        return h;
    }

    public void set_num_hashs(int numHashs){
        h=numHashs;
    }

    public double getCount(){//1的数量
        double percentage = (double) bitSet.cardinality() / m * 100;
        return percentage;
    }
    private int[] hashes(String element) {
        // 生成元素对应的h个哈希函数值
        return Utils.generateHashes(element,h,m);
    }

    public void add(String e) {
        // 添加元素到布隆过滤器中
        int[] indexes = hashes(e);
        for (int index : indexes) {
            bitSet.set(index);
        }
    }

    public boolean test(int[] a) {//查询元素，按列（即按布隆过滤器）查询时使用
        for (int idx : a) {
            if (!bitSet.get(idx)) {
                return false;
            }
        }
        return true;
    }

    public void update(List<String> elements) {
        // 对于给定的元素集合，遍历集合中的每个元素，调用 add 方法依次将其添加到布隆过滤器中
        for (String e : elements) {
            add(e);
        }
    }


}

