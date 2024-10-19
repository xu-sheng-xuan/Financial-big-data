package com.example.words;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordsCount {

    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // 用于存储单词及其出现次数的 Map
        private final static IntWritable one = new IntWritable(1);
        
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        // 从HDFS加载停词表
        protected void setup(Context context) throws IOException, InterruptedException {
            String stopWordsFilePath = context.getConfiguration().get("stopwords.path");
            if (stopWordsFilePath != null) {
                Path path = new Path(stopWordsFilePath);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase()); // 加载停词表，忽略大小写
                }
                reader.close();
            }
        }
        //map方法读取每行数据，使用,分割字符串，获取headline列。将headline列的单词转换为小写，去掉标点符号，然后使用空格分割单词。将单词作为Text类型的键输出，并将初始计数设置为1（使用IntWritable）。
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 1) {
                // 对 headline 列进行处理，去掉标点符号，转换为小写
                String headline = fields[1].toLowerCase().replaceAll("[^a-zA-Z ]", " ");
                for (String token : headline.split("\\s+")) {
                    // 忽略停词和空字符串
                    if (!stopWords.contains(token) && token.length() > 0) {
                        word.set(token);
                        context.write(word, one);
                    }
                }
            }
        }
    }
    //reduce方法计算每个单词出现的次数，将单词作为Text类型的键输出，并将计数作为IntWritable类型的值输出。
    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 用来存储单词及其出现次数的 Map
        private Map<String, Integer> wordCountMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //value是单词，key是次数
            wordCountMap.put(key.toString(), sum); 
            // 保存每个单词及其出现次数
        }

        @Override
        //cleanup方法将map转换为list，按照出现次数从大到小排序，输出前100个高频词，格式为 "<排名>：<单词>，<次数>"
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将 map 转换为 list 以便排序
            List<Map.Entry<String, Integer>> sortedWords = new ArrayList<>(wordCountMap.entrySet());

            // 按照出现次数从大到小排序
            sortedWords.sort((o1, o2) -> o2.getValue().compareTo(o1.getValue()));

            // 输出前100个高频词，格式为 "<排名>：<单词>，<次数>"
            int rank = 1;
            for (int i = 0; i < Math.min(100, sortedWords.size()); i++) {
                Map.Entry<String, Integer> entry = sortedWords.get(i);
                context.write(new Text(rank + ": " + entry.getKey()), new IntWritable(entry.getValue()));
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 设置 stop-word-list.txt 的 HDFS 路径
        conf.set("stopwords.path", args[2]); // 假设第三个参数是停词表路径
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordsCount.class);
        job.setMapperClass(WordMapper.class);
        job.setReducerClass(WordReducer.class); // 设置 reducer 类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}