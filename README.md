本次实验目的为：
  在HDFS上加载上市公司热点新闻标题数据集（analyst_ratings.csv），该数据集收集了部分上市公司的热点财经新闻标题。编写MapReduce程序完成以下两项任务：
  1. 统计数据集上市公司股票代码（“stock”列）的出现次数，按出现次数从大到小输出，输出格式为"<排名>：<股票代码>，<次数>“；
  2. 统计数据集热点新闻标题（“headline”列）中出现的前100个高频单词，按出现次数从大到小输出。要求忽略大小写，忽略标点符号，忽略停词（stop-word-list.txt）。输出格式为"<排名>：<单词>，<次数>“。
对于实验1：
下面按类阐述设计思路：
1. StockCount 类
这是主类，包含了两个内部类 StockMapper 和 StockReducer，以及一个 main 方法来配置和运行 MapReduce 作业。
2. StockMapper 类
这是 MapReduce 作业的 Mapper 类。它的任务是处理输入数据，提取出股票代码，并为每个股票代码输出一个初始计数为 1 的键值对。
map 方法：这是 Mapper 的核心方法，它读取每行输入数据，使用逗号 , 分割字符串，然后提取最后一个元素作为股票代码。股票代码被设置为输出键（Text 类型），并将初始计数设置为 1（使用 IntWritable 类型）。
此是key-value输出，key是股票代码，value是1
备注：之前直接取fields[4]结果老是出错，后来发现是因为headline中也存在逗号，而hadoop对csv文件的划分就是根据逗号来划分的，故会出错，后来直接取length-1即可
4. StockReducer 类
这是 MapReduce 作业的 Reducer 类。它的任务是接收来自 Mapper 的输出，对每个股票代码的出现次数进行累加，并在最后输出排序后的股票代码及其出现次数。
reduce 方法：这个方法接收一个股票代码和该代码对应的所有计数（由 Mapper 输出）。它将这些计数累加，并将结果存储在一个 HashMap 中。
cleanup 方法：在所有的 reduce 任务完成后，这个方法会被调用。它将 HashMap 转换为 ArrayList，然后根据出现次数对股票代码进行排序。最后，它输出排序后的股票代码及其出现次数，格式为“<排名>：<股票代码>，<次数>”。
5. main 方法
这是程序的入口点，它配置并运行 MapReduce 作业。
配置作业：设置作业的名称、Jar 文件、Mapper 和 Reducer 类，以及输入输出键值对的类型。
设置输入输出路径：使用 args 参数指定输入和输出路径。调用代码为hadoop jar target/StockCount.jar com.example.stock.StockCount /homework/data/analyst_ratings.csv /src/main/output/words
对于实验2：
2. WordsCount 类
这是主类，包含了两个内部类 WordMapper 和 WordReducer，以及一个 main 方法来配置和运行 MapReduce 作业。
3. WordMapper 类
这是 MapReduce 作业的 Mapper 类。它的任务是处理输入数据，提取出单词，并为每个单词输出一个初始计数为 1 的键值对
setup 方法：这是 Mapper 的初始化方法，用于从 HDFS 加载停用词表。停用词表的路径通过配置参数 stopwords.path 传递。Mapper 读取停用词表，并将每个停用词存储在一个 HashSet 中，以便快速查找。
map 方法：这是 Mapper 的核心方法，它读取每行输入数据，分割字符串获取标题列（假设标题列是第二列），然后对标题列进行处理，包括转换为小写、去掉标点符号，并使用空格分割单词。对于每个非空且非停用词的单词，Mapper 输出一个键值对，其中键是单词（Text 类型），值是初始计数 1（IntWritable 类型）。
4. WordReducer 类
这是 MapReduce 作业的 Reducer 类。它的任务是接收来自 Mapper 的输出，对每个单词的出现次数进行累加，并在最后输出排序后的高频词。
reduce 方法：这个方法接收一个单词和该单词对应的所有计数（由 Mapper 输出）。它将这些计数累加，并将结果存储在一个 HashMap 中。
cleanup 方法：在所有的 reduce 任务完成后，这个方法会被调用。它将 HashMap 转换为 ArrayList，然后根据出现次数对单词进行排序。最后，它输出前 100 个高频词，格式为 "<排名>: <单词>，<次数>"。
5. main 方法
这是程序的入口点，它配置并运行 MapReduce 作业。
配置作业：设置作业的名称、Jar 文件、Mapper 和 Reducer 类，以及输入输出键值对的类型。
设置 stop words 文件路径：通过 args 参数指定停用词表的 HDFS 路径，并将其设置在作业的配置中。
设置输入输出路径：使用 args 参数指定输入和输出路径。调用路径为hadoop jar src/main/java/com/example/words/WordsCount.jar com.example.words.WordsCount /homework/data/analyst_ratings.csv /src/main/output/words /homework/data/stop-word-list.txt wordcount
成功结果如图
调用流程如下：
首先将analyst_rating.csv 和 stop-word-list.txt用docker cp从本地上传到docker容器里面，然后用hadoop fs -put放到hdfs文件系统里面，接着用mvn clean package打包生成jar，后用上述调用步骤调用执行jar，最后
显示是否输出成功
hdfs dfs -ls /src/main/output/stocks
hdfs dfs -ls /src/main/output/words
输出结果
hdfs dfs -cat /src/main/output/stocks/*
hdfs dfs -cat /src/main/output/words/*
然后用 hdfs dfs -get /src/main/output/words /home/homework/homework1.1/demo/src/main/java/com/example/output将文件保存到本地
注意多次重复执行时要想用hdfs rm将上次生成文件删去才能重复执行。
