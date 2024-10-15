package com.example.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class StockAndHeadlineMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text stock = new Text();
    private Text headline = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length > 1) {
            stock.set(parts[1].trim());
            context.write(stock, NullWritable.get());
        }
        if (parts.length > 3) {
            headline.set(parts[3].trim());
            context.write(headline, NullWritable.get());
        }
    }
}