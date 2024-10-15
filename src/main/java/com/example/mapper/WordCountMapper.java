
package com.example.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();
    private final Set<String> stopWords = new HashSet<>();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        // Load stop words from file
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        // Remove punctuation
        for (String token : line.split("\\W+")) {
            if (!stopWords.contains(token) && !token.isEmpty()) {
                word.set(token);
                context.write(word, one);
            }
        }
    }
}
