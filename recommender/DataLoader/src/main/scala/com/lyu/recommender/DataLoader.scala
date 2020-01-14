package com.lyu.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 数据：Product 数据集 分隔符：^
 * 商品ID
 * 商品名称
 * 商品分类ID， 不需要
 * 亚马逊ID， 不需要
 * 商品图片URL
 * 商品分类
 * 商品UGC标签
 **/

/**
 * 定义 product 样例类 -- 对原始数据进行字段筛选
 * @param productId
 * @param name
 * @param imageUrl
 * @param categories
 * @param tags  商品UGC标签
 */
case class Product( productId: Int, name: String, imageUrl: String, categories: String, tags: String )

/**
 * 数据：Rating 数据集 分隔符：,
 * 用户ID
 * 商品ID
 * 评分
 * 时间戳
 */

/**
 * 定义 Rating 样例类
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

object DataLoader {
  // 定义常量，如路径，mongoDB中存储的表名等
//  val PRODUCT_DATA_PATH = "G:\\demo\\DEMO_2\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
//  val RATING_DATA_PATH = "G:\\demo\\DEMO_2\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {

    // 配置选项
    val config = Map(
      "spark.cores"-> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建一个 spark session  (-->sql)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 操作DF DS需要引入 implicits
    import spark.implicits._

    // 加载数据 -- 从文件中加载数据  ^需要转义，反斜杠\也需要转义
//    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    // TODO: args(0):val PRODUCT_DATA_PATH = "G:\\demo\\DEMO_2\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
    val productRDD = spark.sparkContext.textFile(args(0))
    val productDF = productRDD.map(item => {
      // 切分数据
      val attr = item.split("\\^")
      // 转换成 自定义的样例类格式 Product() , 返回 (scala中最后一行表示return)
      // 类型还是 RDD, 需要 RDD --> DF
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

//    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    // TODO: args(1):val RATING_DATA_PATH = "G:\\demo\\DEMO_2\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
    val ratingRDD = spark.sparkContext.textFile(args(1))
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // 通过样例类配置 MongoDB 隐式配置对象
    // TODO: 为什么需要 implicit, 去掉关键字 implicit 会有什么影响？？
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    // 存储：参数--数据及对应的隐式配置项
    storeDataInMongoDB(productDF, ratingDF)

    spark.stop()
  }

  // TODO: 方法定义根据业务逻辑来，需要将 数据DF 存储到 mongoDB 中，因此传入的参数有 DF,同时需要数据库连接配置参数
  // TODO: 这里数据库连接配置参数为隐式(implicit)参数，因此传参直接 (implicit xxxx:xxxx),在调用方法是只需要前面()中的参数即可
  /**
   * mongodb驱动：com.mongodb.casbah
   * function: 一次性将 DF 数据全部写入到数据库
   * @param productDF
   * @param ratingDF
   * @param mongoConfig  implicit
   */
  def storeDataInMongoDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 1.新建一个 mongodb 的连接, 客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    // 2.定义操作的mongodb中的表 db.Product
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // 3.如果表已经存在，则删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // 4.将当前数据存入到对应的表中,option包含的参数需要查看对应mongodb在该方法中的参数定义
    // 方法1：write直接保存数据 -- DF.write.option().mode().format().save() ：该方法 .save不可缺少
    // 方法2：format指定保存数据类型 -- 详情见笔记，该方法不需要 .save --> 如 mysql: DF.write.jdbc(uri, db, props)
    productDF.write
      .option("uri", mongoConfig.uri)  /*数据库连接*/
      .option("collection", MONGODB_PRODUCT_COLLECTION) /*数据写入的表名*/
      .mode("overwrite")  /*写入的模式*/
      .format("com.mongodb.spark.sql") /*写入格式：如果不是内置格式，需要写全称*/
      .save() /*写入后保存*/

    ratingDF.write
      .option("uri", mongoConfig.uri)  /*数据库连接*/
      .option("collection", MONGODB_RATING_COLLECTION) /*数据写入的表名*/
      .mode("overwrite")  /*写入的模式*/
      .format("com.mongodb.spark.sql") /*写入格式：如果不是内置格式，需要写全称*/
      .save() /*写入后保存*/

    // 5.对表创建索引
    // TODO: mongodb为什么需要创建索引？？？ 1表示升序排列？？
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))

  }

}
