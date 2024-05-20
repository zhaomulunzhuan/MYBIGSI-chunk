package com.chen;
import com.google.common.hash.Hashing;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Utils {
    public static int hash(String element, int seed, int m) {
        // 返回一个哈希值，该哈希值被限制在范围 [0, m) 内
        int hash = Hashing.murmur3_32(seed).hashUnencodedChars(element).asInt();
        return (hash & 0x7FFFFFFF) % m;
    }

    public static int[] generateHashes(String element, int numberHashFunctions, int bloomFilterSize) {//元素，哈希函数个数，布隆过滤器大小
        int[] hashes = new int[numberHashFunctions];
        for (int seed = 0; seed < numberHashFunctions; seed++) {
            hashes[seed] = hash(element, seed, bloomFilterSize);
        }
        return hashes;
    }

    public static List<String> readInputFiles(String filePath) throws IOException {
        List<String> inputFiles = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                inputFiles.add(line);
            }
        }

        return inputFiles;
    }
    public static List<String> getKmers(String filepath) throws IOException {
        List<String> keys = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(filepath));
        String line;
        while ((line = br.readLine()) != null) {
            keys.add(line);
        }
        br.close();
        return keys;
    }


    public static List<BloomFilter> deserializeBFlist(String filename) {
        List<BloomFilter> list = new ArrayList<>();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            list = (List<BloomFilter>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return list;
    }



}
