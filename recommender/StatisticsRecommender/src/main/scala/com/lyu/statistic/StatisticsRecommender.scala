package com.lyu.statistic

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 定义 Rating 样例类
 *
 * @param userId
 * @param productId
 * @param score
 * @param timestamp
 */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )

/**
 * MongoDB 连接配置 样例类
 * @param uri   连接uri
 * @param db    要操作的数据库
 */
case class MongoConfig( uri: String, db: String )

object StatisticsRecommender {
  // 定义常量，如路径，mongoDB中存储的表名等
  val MONGODB_RATING_COLLECTION = "Rating"
  // 统计表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    // 配置选项
    val config = Map(
      "spark.cores"-> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建一个 spark session  (-->sql)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 操作DF DS需要引入 implicits
    import spark.implicits._
    // 通过样例类配置 MongoDB 隐式配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据: 从 mongodb 数据库中加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating] /*Dataset*/
      .toDF() /*DataSet --> DataFrame*/

    // 创建一张 ratings 临时表 DF --> MYSQL， 用于使用 sql 语句查询
    ratingDF.createOrReplaceTempView("ratings")

    // TODO: 用 spark sql 做不同的统计推荐
    // 1.历史热门商品，按照评分个数统计 --> productId, count
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2.近期热门商品，将 timestamp 转换为 yyyymm 格式进行评分个数统计
    // 创建日期格式化工具(java中的方法)
    val simpleDateFormat = new SimpleDateFormat("yyyymm")
    // TODO: 注册 UDF --- 转换 timestamp 为 yyyymm 格式 (UDF 用法详见笔记)
    spark.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 原始 rating 数据转换成想要的结构 productId, score, yearmonth
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
    // 创建临时表
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    // 从临时表中select 近期热门商品
    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
    // 保存　近期热门商品　至　mongodb　的 RateMoreRecentlyProducts　表中
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 3.优质商品统计，商品的平均评分
    val averageProductsDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    // println　averageProductsDF 是降序排列的，数据库中不是
    averageProductsDF.take(5).foreach(println)

    spark.stop()

  }

  /**
   * function:将 DataFrame 格式的数据写入到指定的 mongodb的collection中
   * @param df
   * @param collection_name
   * @param mongoConfig
   */
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }


}
