package com.lyu.itemCF

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 定义标准推荐对象 productId, score
 *
 * @param productId
 * @param score
 */
case class Recommendation( productId: Int, score: Double )

/**
 * 定义用户的推荐列表 物品两两之间相似度 productId:{productId1:score1, productId2:score2,....}
 * @param productId
 * @param recs
 */
case class ProductRecs( productId: Int,  recs: Seq[Recommendation])

/**
 * 定义 Rating 样例类
 *
 * @param userId
 * @param productId
 * @param score
 * @param timestamp
 */
case class ProductRating( userId: Int, productId: Int, score: Double, timestamp: BigInt )

/**
 * MongoDB 连接配置 样例类
 * @param uri   连接uri
 * @param db    要操作的数据库
 */
case class MongoConfig( uri: String, db: String )


object ItemCFRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      // TODO: 如何远程连接hadoop集群使用 yarn 模式？？ 或者 如何打包提交以 yarn 模式运行？？
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 载入数据 预处理，转换成DF进行处理
    val ratingDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(x => (x.userId, x.productId, x.score))
      .toDF("userId", "productId", "score")
      .cache()
    ratingDF.show(20,false)

    // TODO: 核心算法，计算同现相似度(同时出现在多用户的兴趣列表)，得到商品相似列表
    // 统计每个商品的评分个数，按照productId做聚合
    val productRatingCountDF: DataFrame = ratingDF.groupBy("productId").count()
    productRatingCountDF.show(10, false)
//    +---------+-----+
//    |productId|count|
//    +---------+-----+
//    |505556   |172  |
//    |275707   |364  |
    // 在原有的评分表rating上添加count
    val ratingWithCountDF: DataFrame = ratingDF.join(productRatingCountDF, "productId")
    // 将评分按照用户id凉凉配对，统计两个商品呗同一个用户评分过的次数
    val joinedDF: DataFrame = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId", "product1", "score1", "count1", "product2", "score2", "count2")
      .select("userId", "product1", "count1", "product2", "count2")
    joinedDF.show(10, false)
    // 创建一张临时表，用于写sql查询
    joinedDF.createOrReplaceTempView("joined")
    // 将（product1,product2）作为key做聚合, 统计userId的数量，就是对两个商品同时评分的人数
    val cooccurrenceDF: DataFrame = spark.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
        |
        |""".stripMargin
    ).cache()
    cooccurrenceDF.show(10)

    // 用同现的次数和各自的次数计算同现相似度
    val simDF: DataFrame = cooccurrenceDF.map { row =>
      val coocSim = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
      // 返回值
      (row.getAs[Int]("product1"), (row.getAs[Int]("product2"), coocSim))
    }
      .rdd
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs(
            productId,
            recs.toList
              .filter( x => x._1 != productId )
              .sortWith(_._2>_._2)
              .map( x => Recommendation(x._1, x._2) )
              .take(MAX_RECOMMENDATION)
          )
      }.toDF()
//            // 使用样例类会按照样例类定义的列名生成
//        item =>
//          (
//            item._1,
//            item._2.toSeq
//              .filter(x => x._1 != item._1)
//              .sortWith(_._2 > _._2)
//              .take(MAX_RECOMMENDATION)
//              .map(x => Recommendation(x._1, x._2)))
//      }.toDF()
    simDF.show(20, false)

    // 保存到 mongodb
    simDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", ITEM_CF_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  def cooccurrenceSim(cooCount: Long, count1: Long, count2: Long) = {
    cooCount / (math.sqrt(count1 * count2))
  }

}
