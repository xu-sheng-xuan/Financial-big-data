package com.example.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.TreeMap;

public class WordReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
    private TreeMap<Text, Integer> map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        map.put(key, sum);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int rank = 1;
        for (Text word : map.descendingKeySet()) {
            context.write(new IntWritable(rank++), new Text(word + "," + map.get(word)));
            if (rank > 100) break;
        }
    }
}