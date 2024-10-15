package com.example;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceJob {

    public static class StockCount {
        public static class StockMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] parts = value.toString().split(",");
                if (parts.length > 1) {
                    Text stock = new Text(parts[1].trim());
                    context.write(stock, NullWritable.get());
                }
            }
        }

        public static class StockReducer extends Reducer<Text, NullWritable, IntWritable, Text> {
            @Override
            protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                for (NullWritable val : values) {
                    count++;
                }
                context.write(new IntWritable(count), key);
            }
        }
    }

    public static class WordCount {
        public static class HeadlineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
            private final Text word = new Text();
            private final IntWritable one = new IntWritable(1);
            private static final String STOP_WORDS_FILE = "stop-word-list.txt";
            private final Set<String> stopWords = new HashSet<>();

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {
                super.setup(context);
                // Load stop words from file
                Path stopWordsPath = new Path(STOP_WORDS_FILE);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)))) {
                    String line;
                    while ((line = br.readLine())!= null) {
                        stopWords.add(line.trim());
                    }
                }
            }

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String line = value.toString().toLowerCase();
                // Remove punctuation
                StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?!\"'()-");
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();
                    if (!stopWords.contains(token) &&!token.isEmpty()) {
                        word.set(token);
                        context.write(word, one);
                    }
                }
            }
        }

        public static class HeadlineReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
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
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job for stock count
        Job stockJob = Job.getInstance(conf, "stock count");
        stockJob.setJarByClass(MapReduceJob.class);
        stockJob.setMapperClass(StockCount.StockMapper.class);
        stockJob.setReducerClass(StockCount.StockReducer.class);
        stockJob.setOutputKeyClass(Text.class);
        stockJob.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(stockJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(stockJob, new Path(args[1]));

        // Job for word count
        Job wordJob = Job.getInstance(conf, "word count");
        wordJob.setJarByClass(MapReduceJob.class);
        wordJob.setMapperClass(WordCount.HeadlineMapper.class);
        wordJob.setReducerClass(WordCount.HeadlineReducer.class);
        wordJob.setOutputKeyClass(Text.class);
        wordJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wordJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(wordJob, new Path(args[3]));

        System.exit(stockJob.waitForCompletion(true) && wordJob.waitForCompletion(true)? 0 : 1);
    }
}