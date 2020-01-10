package com.lyu.online

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @Description: 自定义试试推荐算法
 * @ClassName: OnlineRecommender
 * @Author: klaus
 * @Date: 2019-12-16 09:27
 **/

/**
 * redis 和 mongodb 客户端定义 (懒执行)
 */
object ConnHelper extends Serializable {
  // 懒变量定义，使用时才会初始化
  // TODO: 建议使用redis pool, 且 auth
  lazy val jedis = new Jedis("192.168.13.128", 6379)
  // jedis.auth("spindle123456")
  // mongo client 实现复杂的mongo数据库操作
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://192.168.13.128:27017/recommender"))
}

/**
 * MongoDB 连接配置 样例类
 * @param uri   连接uri
 * @param db    要操作的数据库
 */
case class MongoConfig( uri: String, db: String )

/**
 * 定义标准推荐对象 productId, score
 * @param productId
 * @param score
 */
case class Recommendation( productId: Int, score: Double )

/**
 * 定义用户推荐列表 userId:{productId1:score1, productId2:score2,....}
 * @param userId
 * @param recs
 */
case class UserRecs( userId: Int, recs: Seq[Recommendation] )

/**
 * 定义用户的推荐列表 物品两两之间相似度 productId:{productId1:score1, productId2:score2,....}
 * @param productId
 * @param recs
 */
case class ProductRecs( productId: Int,  recs: Seq[Recommendation])

object OnlineRecommender {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"
  val MAX_USER_RATING_NUM = 20  /*从redis中最多选取的评分数*/
  val MAX_SIM_PRODUCTS_NUM = 20 /*备选商品数量的最大值*/

  def main(args: Array[String]): Unit = {
    // 配置选项
    val config = Map(
      // TODO: 如何远程连接hadoop集群使用 yarn 模式？？ 或者 如何打包提交以 yarn 模式运行？？
      // redis 需要更多参数的话也可以在这里定义
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // 创建spark config
    val sparkConf: SparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // spark streaming: StreamingContext的参数可以使 sparkConf, 也可以是sparkContext
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据--相似度矩阵, 由于数据量大, 可采用广播的方式发送给每一个 executer, 而不是直接发送给每一个Task
    val simProductsMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .format("com.mongodb.spark.sql")
      .load() /*DataFrame*/
      .as[ProductRecs] /*Dataset[Product], 转化为 嵌套map可以优化查询*/
      .rdd /* RDD[Product], 使用RDD的map算子实现嵌套map的转换 p -> (p -> v) */
      .map{ item =>
        // TODO: RDD[Product] --> RDD[(Int, Map[Int, Double])]
        // 内层每一条item的[productId, score]列表的每一个成对元素转化成 map (p -> v)
        (item.productId, item.recs.map(x => (x.productId, x.score)).toMap) /*toMap需要是从tuple转*/
      } /* RDD[(Int, Map[Int, Double])] */
      // TODO: RDD[(Int, Map[Int, Double])] --> Map[(Int, Map[Int, Double])]
      // Return the key-value pairs in this RDD to the master as a Map
      .collectAsMap()  /*collection.Map[Int, Map[Int, Double]]*/
    // 定义广播变量
    val simProductsMatrixBC: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

    // 创建 kafka 配置参数
    val kafkaParams = Map(
      "bootstrap.servers" -> "192.168.13.128:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"   /*偏移量设置*/
      //"enable.auto.commit" -> "false"
    )

    // 常见 DStream createDirectStream需要泛型, Subscirbe也需要泛型
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      // Subscribe的topics类型需要注意
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParams)
    )

    // 对 kafkaStream 进行处理，产生评分流, userId|productId|score|timestamp
    val ratingStream: DStream[(Int, Int, Double, Int)] = kafkaStream.map{ msg =>
      val attr: Array[String] = msg.value().split("\\|")
      (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    ratingStream.foreachRDD{rdds=>rdds.foreach(println)}    /*(4867,8195,4.0,157674078)*/

    // 核心算法部分，定义评分流的处理流程
    ratingStream.foreachRDD{ rdds =>
      rdds.foreach{
        // 模式匹配为元祖
        case (userId, productId, score, timestamp) =>
          println("rating data coming! >>>>>>>>>>>>>>>>>>>>>>>>>")
          // TODO: 核心算法部分流程
          // 1. 从redis中获取当前用户的最近k次评分, 保存为一个数组 Array[(productId, score)]
          val userRecentlyRatings = getUserRecentlyRatings( userId, MAX_USER_RATING_NUM, ConnHelper.jedis )

          // 2. 从广播变量simproductsMatrixBC相似度矩阵中获取当前商品的最相似商品列表，作为备选列表,保存为一个数组 Array(productId)
          // 备选商品筛选，需要去除掉当前用户评过分的商品（推荐肯定是推荐当前用户没评过分的）, 通过Rating表中的userId来筛选
          val candidateProducts = getCandidateProducts( userId, productId,  simProductsMatrixBC.value, MAX_SIM_PRODUCTS_NUM)

          // 3. 计算每个商品的推荐优先级，得到当前用户的实时推荐列表, 保存为数组 Array[(productId, recScore)]
          val streamRecs = computerProductScore( candidateProducts, userRecentlyRatings, simProductsMatrixBC.value )

          // 4. 把推荐列表保存到 mongodb
          saveRecsToMongoDB( userId, streamRecs )
      }
    }
    // 启动streaming
    ssc.start()
    println("streaming started!")
    ssc.awaitTermination()

  }

  import scala.collection.JavaConversions._ /*操作java的数据结构时需要*/
  /**
   * 从 redis 中用户的评分队列里获得最近评分数据, list 键名为 userId:USERID，值格式是 PRODUCTID:SCORE
   * 比如 set userID:5864 6584635:5.4 (其中key=userID:5864, value=6584635:5.4)
   * @param userId
   * @param num
   * @param jedis
   * @return
   */
  def getUserRecentlyRatings(userId: Int, num: Int, jedis: Jedis): Array[(Int, Double)] = {
    // jedis.lrange(key,start,end): Return the specified elements of the list stored at the specified key
    jedis.lrange("userId:" + userId.toString, 0, num) /* Java的LIST操作, key 为 "userId:5584" ,value 为 "PRODUCTID:5.2" */
      .map{ item => /* 对 key"userId:xxx" 对应的 value值 "PRODUCTID:xxx" 按照 : 切分, : 需要转义符号 */
        val attr = item.split("\\:")
        ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray  /* 将java数据结构转化为scala数据结构 */
  }

  /**
   * 从当前商品的相似度列表中去除掉已经评分过的商品，得到候选商品列表
   * @param userId      当前用户的Id
   * @param productId   当前商品的Id
   * @param simProducts 当前商品的相似度列表
   * @param num         top_num 取出作为候选商品
   * @param mongoConfig
   * @return  候选商品 Array[productId: Int]
   */
  def getCandidateProducts(userId: Int,
                           productId: Int,
                           simProducts: collection.Map[Int, collection.Map[Int, Double]],
                           num: Int)
                          (implicit mongoConfig: MongoConfig):Array[Int] = {
    // 1. 从Rating表中取出 已经评分过的商品, ratedProducts, 过滤掉，排序输出
    val ratingCollection: MongoCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    val ratedProducts: Array[Int] = ratingCollection.find(MongoDBObject("userId" -> userId)) /* 结果类型：MongoCursor; 查询的筛选条件 userId*/
      .toArray /*Array[Imports.DBObject]*/
      .map {item => /* item有4个字段, userId, productId, score, timestamp; 只需要取 productId */
        item.get("productId").toString.toInt
      }

    // 2. 从广播变量相似度矩阵中获取当前商品的相似度列表 Array[(Int, Double)], (productId, score)
    val allSimProducts: Array[(Int, Double)] = simProducts(productId).toArray

    // 3. 从 所有的相似度列表中 筛选去除掉 已经评分过的商品, 排序后取前num个 --> 候选商品列表
    allSimProducts.filter( x => ! ratedProducts.contains(x._1) )
      .sortWith(_._2 > _._2) /*降序排列*/
      .take(num) /* 取 top_num 个商品数据记录 */
      .map(x => x._1)  /*只取出其中的 productId 即可*/
  }

  /**
   * 根据公式计算推荐优先级，并返回最终推荐列表 (productId, recScore)
   * @param candidateProducts
   * @param userRecentlyRatings
   * @param simProducts
   * @return
   */
  def computerProductScore(candidateProducts: Array[Int],
                           userRecentlyRatings: Array[(Int, Double)],
                           simProducts: collection.Map[Int, collection.Map[Int, Double]]):Array[(Int, Double)] = {
    // 定义一个长度可变的数组 ArrayBuffer 保存每一个备选商品的基础得分 (productId, score)  (Array数组长度不可变)
    val scores: ArrayBuffer[(Int, Double)] = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义一个map 保存每个商品高分和低分的计数器 (productId -> count)
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    // 遍历每个备选商品，计算与每一个已评分商品的相似度 (scala中嵌套循环可以放在一起)
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
      // 从相似度矩阵中获取当前商品和当前已评分商品见的相似度
      val simScore = getProductsSimScore( candidateProduct,  userRecentlyRating._1, simProducts)

      if ( simScore > 0.4 ){
        // 1). 按照公式加权计算, 得到基础得分
        // += 是一个函数def, 紧跟着的()是函数调用的括号，如果传参是元组, 需要 +=((key,value))
        scores += ( ( candidateProduct, simScore * userRecentlyRating._2 ) )
        // 2). 惩罚项和奖励项的计数器
        if (userRecentlyRating._2 > 3) {
          /*getOrDefault(key, value_default):key存在则返回get(key); 否则返回默认值0 */
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // 根据公式计算所有候选商品的推荐优先级，productId聚合
    scores.groupBy(_._1) /* Map[Int, ArrayBuffer[(Int, Double)]] */
      .map {
        case (productId, scoreList) =>
          (productId,
           scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
      }
      .toArray
      .sortWith(_._2>_._2)
  }

  /**
   * 从相似度矩阵中获取 两个商品之间的相似度值
   * @param product1
   * @param product2
   * @param simProducts
   * @return
   */
  def getProductsSimScore(product1: Int,
                          product2: Int,
                          simProducts: collection.Map[Int, collection.Map[Int, Double]]): Double = {
    // Scala集合类型（List,Map等）操作的返回类型是Option, Option有两个子类别 Some和None.
    simProducts.get(product1) match { /* Option[collection.Map[Int, Double]]*/
      case Some(sims) => sims.get(product2) match {  /* Option[Double] */
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  /**
   * 将用户的试试推荐结果保存至mongodb中
   * @param userId
   * @param streamRecs
   * @param mongoConfig
   */
  def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS)

    // 查询userId并删除对应内容，然后再插入实时处理后的数据
    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(
      MongoDBObject(
        "userId" -> userId,
        "recs" -> streamRecs.map(x => MongoDBObject("productId"->x._1, "score"->x._2))
      )
    )
  }

  /**
   * 换底公式  java的log 以 e为底, 利用换底公式换乘以10为底
   * @param m
   * @return
   */
  def log(m: Int): Double = {
    val N=10
    math.log(m) / math.log(N)
  }


}
