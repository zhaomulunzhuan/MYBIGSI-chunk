package com.chen;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class testLong {
    private static long[] storage_row;//转置后的按行存储

    public static void main(String[] args) {
        List<boolean[]> bitArrays = new ArrayList<>();
        for (int i = 0; i < 65; i++) {
            boolean[] array = new boolean[3];
            for (int j = 0; j < 3; j++) {
                array[j] = i % 2 == 0;
            }
            bitArrays.add(array);
        }

        boolean[] array = new boolean[3];
        for (int j = 0; j < 3; j++) {
            array[j]=true;
        }
        bitArrays.add(array);

        transposeBloomFiltersToLongArray(bitArrays);

//        System.out.println("Transposed Long Array Bits:");
//        for (int i = 0; i < storage_row.length; i++) {
//            for (int j = 0; j < Long.SIZE; j++) {
//                long mask = 1L << j;
//                boolean bit = (storage_row[i] & mask) != 0;
//                System.out.println("storage_row[" + i + "][" + j + "]: " + (bit ? "1" : "0"));
//            }
//        }


    }


    public static void transposeBloomFiltersToLongArray(List<boolean[]> bloomFilters) {
        long numCols = bloomFilters.size();
//        long numRows = Metadata.getBloomfilterSize();

        long numRows=3;
        System.out.println(numCols);
        System.out.println(numRows);
        // 计算所需的long数组的长度，每个long类型可以存储64位数据
        int arrayLength = (int) Math.ceil((double) (numRows * numCols) / Long.SIZE);
        System.out.println(arrayLength);

        storage_row = new long[arrayLength];

        for (int i = 0; i < numRows; i++) {
            for (int j = 0; j < numCols; j++) {
                boolean bit = bloomFilters.get(j)[i];
                if (bit) {
                    // 计算当前位在long数组中的索引和偏移量
                    int index = (int) ((i * numCols + j) / Long.SIZE);
                    int offset = (int) ((i * numCols + j) % Long.SIZE);
                    // 将对应的位设置为1
                    storage_row[index] |= (1L << offset);
                }
            }
        }


        BitSet result=new BitSet((int) numCols);
        result.set(0, (int) numCols);//都设置为1
        BitSet tempbitset=new BitSet((int) numCols);

        int[] indexes={0, 2};

        for(int index:indexes){
            long start_index= (long) index *numCols;//哈希到的这一行的一个元素的bit索引
            long end_index=start_index+numCols-1;
            System.out.println("第"+index+"行第一个比特在矩阵中对应的索引"+start_index);
            System.out.println("第"+index+"最后一个比特在矩阵中对应的索引"+end_index);


            long start_long_index=start_index/Long.SIZE;//开始要取的第一个bit所在的long索引
            long start_bit_index=start_index%Long.SIZE;

            long end_long_index=end_index/Long.SIZE;//要取的最后一个bit所在的long索引
            long end_bit_index=end_index%Long.SIZE;

            System.out.println("获取的区域");
            System.out.println(start_long_index+"个long中"+start_bit_index+"位到"+end_long_index+"个long中"+end_bit_index+"位");

            //获取start_long_index到end_long_index这（end_long_index-start_long_index+1）个long
            //将start_long_index中的start_bit_index一直到end_long_index中的end_bit_index存入tempbitset中
            // 获取start_long_index到end_long_index这（end_long_index-start_long_index+1）个long
            for (long i = start_long_index; i <= end_long_index; i++) {
                long start_bit = (i == start_long_index) ? start_bit_index : 0;
                long end_bit = (i == end_long_index) ? end_bit_index : Long.SIZE - 1;
                System.out.println("在"+i+"long中的开始bit："+start_bit);
                System.out.println("在"+i+"long中的结束bit："+end_bit);

                long mask;
                if (end_bit==63){
                    mask = 0xFFFFFFFFFFFFFFFFL-((1L << start_bit) - 1L);
                }else{
                    mask = (1L << (end_bit + 1)) - (1L << start_bit);
                }
//                long mask = (1L << (end_bit + 1)) - (1L << start_bit);
//                long mask = ((1L << (end_bit - start_bit + 1)) - 1) << start_bit;
                System.out.println("Bits of the mask: " + mask + ":");

                for (int j = 63; j >= 0; j--) {
                    long bitValue = (mask >> j) & 1;
                    System.out.print(bitValue);
                }
                System.out.println();

                long value = storage_row[(int) i] & mask;

                // 将value中的bit写入tempbitset中
                for (long j = start_bit; j <= end_bit; j++) {
                    if ((value & (1L << j)) != 0) {
                        tempbitset.set((int) ((i - start_long_index) * Long.SIZE + j));
                    }
                }
            }

            System.out.println(index+"行"+"结果:");
            for(int i=0;i<numCols;i++){
                if (tempbitset.get(i)){
                    System.out.print(1);
                }else{
                    System.out.print(0);
                }
            }
            System.out.println();
            result.and(tempbitset);

            System.out.println("最终结果");
            for(int i=0;i<numCols;i++){
                if (result.get(i)){
                    System.out.print(1);
                }else{
                    System.out.print(0);
                }
            }
            System.out.println();
        }

    }
}
