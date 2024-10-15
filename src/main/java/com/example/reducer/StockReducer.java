package com.example.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StockReducer extends Reducer<Text, NullWritable, IntWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (NullWritable val : values) {
            count++;
        }
        context.write(new IntWritable(count), key);
    }
}