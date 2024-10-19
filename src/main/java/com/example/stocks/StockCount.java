package com.example.stock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCount {

    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stock = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 分割输入数据
            //map方法读取每行数据，使用,分割字符串，获取最后一个元素作为股票代码。将股票代码作为Text类型的键输出，并将初始计数设置为1（使用IntWritable）。
            String[] fields = value.toString().split(",");
            if (fields.length > 3) { // 确保有 stock 列
                // 因为中间标题列有逗号，直接取最后一列作为股票代码
                stock.set(fields[fields.length - 1].trim());
                //此是key-value输出，key是股票代码，value是1
                context.write(stock, one);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, Text> {
        // 使用Map计数
        private Map<String, Integer> stockCountMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            // 计算每个股票代码出现的次数
            for (IntWritable val : values) {
                sum += val.get();
            }
            //value是股票代码，key是次数
            stockCountMap.put(key.toString(), sum); 
            // 保存每个股票代码及其出现次数
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将 map 转换为 list，便于排序
            List<Map.Entry<String, Integer>> sortedStockList = new ArrayList<>(stockCountMap.entrySet());
            
            // 按照出现次数从大到小排序
            Collections.sort(sortedStockList, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            // 输出排序后的股票代码及其出现次数，按排名格式输出
            int rank = 1;
            for (Map.Entry<String, Integer> entry : sortedStockList) {
                String outputValue = rank + "：" + entry.getKey() + "，" + entry.getValue();
                context.write(new Text(outputValue), null);  
                // 输出格式为 "<排名>：<股票代码>，<次数>"
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        // 由于输出格式变化，将输出值类型改为 Text
        job.setOutputValueClass(Text.class); 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}