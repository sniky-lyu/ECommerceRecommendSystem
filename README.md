## ECommerceRecommendSystem
test: share spark project from idea to git

Structure:
ECommerceRecommendSystem
  -- businessServer
  -- recommender
       -- DataLoader
       -- StatisticsRecommender
       -- OfflineReommender
       -- KafkaStreaming
       -- OnlineRecommender
       -- ContentRecommender
       -- ItemCFRecommender

### >>>>>>>>>>>>>>>>>>>>>>>>>>>>> Commander <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
1.上传 xxx-jar-with-dependencies.jar 到服务器
[centos@master lyu]$ ll
total 785104
-rw-rw-r--. 1 centos centos 135138854 Jan 13 20:26 ALSOptimization-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos 135142334 Jan 13 20:26 ContentRecommender-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos  92043896 Jan 13 20:26 DataLoader-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos  91719914 Jan 13 20:26 ItemCFRecommender-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos  20278937 Jan 13 20:26 KafkaStreaming-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos 135158406 Jan 13 20:26 offlineRecommender-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos 101437075 Jan 13 20:26 OnlineRecommender-jar-with-dependencies.jar
-rw-rw-r--. 1 centos centos     25703 Jan 13 19:15 products.csv
-rw-rw-r--. 1 centos centos   1282497 Jan 13 19:15 ratings.csv
-rw-rw-r--. 1 centos centos  91703813 Jan 13 20:26 StatisticsRecommender-jar-with-dependencies.jar

2. hdfs操作：上传products.csv和ratings.csv到hdfs中
[centos@master hadoop]$ ./bin/hdfs dfs -mkdir -p /lyu/project/erecomsys
[centos@master hadoop]$ ./bin/hdfs dfs -ls /lyu/project/erecomsys
[centos@master hadoop]$ ./bin/hdfs dfs -put /usr/local/spark211/lyu/products.csv /lyu/project/erecomsys
[centos@master hadoop]$ ./bin/hdfs dfs -put /usr/local/spark211/lyu/ratings.csv /lyu/project/erecomsys
[centos@master hadoop]$ ./bin/hdfs dfs -ls /lyu/project/erecomsys
Found 2 items
-rw-r--r--   1 centos supergroup      25703 2020-01-13 20:56 /lyu/project/erecomsys/products.csv
-rw-r--r--   1 centos supergroup    1282497 2020-01-13 20:56 /lyu/project/erecomsys/ratings.csv

3. 提交jar包运行 （local[*]模式运行 -- 代码中设置了 .setMaster("local[*]")）
[centos@master spark211]$ ./bin/spark-submit --class com.lyu.recommender.DataLoader ./lyu/DataLoader-jar-with-dependencies.jar /lyu/project/erecomsys/products.csv /lyu/project/erecomsys/ratings.csv
[centos@master spark211]$ ./bin/spark-submit --class com.lyu.statistic.StatisticsRecommender ./lyu/StatisticsRecommender-jar-with-dependencies.jar
实时推荐：
1) 环境开启：hadoop, yarn, zookeeper, kafka, flume-log-kafka, mongo, redis
2) 运行实时推荐程序：
[centos@master spark211]$ ./bin/spark-submit --class com.lyu.online.OnlineRecommender ./lyu/OnlineRecommender-jar-with-dependencies.jar
3) 运行kafka流式处理程序 (java程序)
[centos@master spark211]$ java -cp ./lyu/KafkaStreaming-jar-with-dependencies.jar com.lyu.kafkastream.Application 192.168.13.128:9092 192.168.13.128:2181 log recommender
4) 开启业务模块(不熟悉，忽略)

4. 提交itemCF和contentCF的jar包运行
[centos@master spark211]$ ./bin/spark-submit --class com.lyu.itemCF.ItemCFRecommender ./lyu/ItemCFRecommender-jar-with-dependencies.jar 
[centos@master spark211]$ ./bin/spark-submit --class com.lyu.content.ContentRecommender ./lyu/ContentRecommender-jar-with-dependencies.jar 

5. mongoDB数据库 recommender 中的 collecions
    > show tables
    AverageProducts
    ContentBasedProductRecs
    ItemCFProductRecs
    OptedParams
    Product
    ProductRecs
    RateMoreProducts
    RateMoreRecentlyProducts
    Rating
    UserRecs
    >

6. 测试完后关闭服务, 可写一个脚本一键关闭


### >>>>>>>>>>>>>>>>>>>>>>>>>>>>> NOTES <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
1) spark-submit命令中若有路径参数则必须是 hdfs 中的路径，不是宿主机本地路径

2) spark-submit 命令常用options如下：
./bin/spark-submit \
	--class com.lyu.test.WordCount \	# 全类名 .scala 程序中object名 右键 CopyReference
	--master yarn \                     # 模式，3种 本地模式, yarn模式, spark集群模式
	--deploy-mode cluster 
	./upload-lyu/WordCount-jar-with-dependencies.jar \
	/lyu/input/word.txt \			    # hdfs的文件目录 <----------------- 注意这里的文件路径是hdfs路径
	/lyu/output/wordcount-output	    # hdfs的文件目录，wordcount-output在运行前必须不能存在

3) 程序中设置的 .setMaster(...) 级别优先于 --master 级别，因此若使用 yarn 模式, 在程序代码中需要注释掉 .setMaster("local[*]"), 按照2)中的options设置运行模式参数

4) 


### >>>>>>>>>>>>>>>>>>>>>>>>>>>>> TODO <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
todo-list:
1) 打jar包提交到集群环境测试部署 --> DONE
2) 数据库加密, 主要是redis加密
3) 代码结构优化，configure类统一管理 