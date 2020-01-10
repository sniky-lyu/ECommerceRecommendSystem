package com.lyu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
 * 基于隐语义模型(LFM)的协同过滤推荐算法
 * 先运行完 ALSOptimization 程序获取最优参数并上传至 mongodb 数据库，offline推荐时读取最优参数构建模型
 * @ClassName: OfflineRecommender
 * @Author: klaus
 * @Date: 2019-12-14 09:19
 **/

/**
 * 定义 Rating 样例类
 *
 * @param userId
 * @param productId
 * @param score
 * @param timestamp
 */
case class ProductRating( userId: Int, productId: Int, score: Double, timestamp: BigInt )

///**
// * MongoDB 连接配置 样例类
// * @param uri   连接uri
// * @param db    要操作的数据库
// */
//case class MongoConfig( uri: String, db: String )

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

///**
// * 定义最优参数样例类
// * @param impFeatureNum
// * @param iterations
// * @param lambda
// * @param rmse
// */
//case class OptedParams( impFeatureNum: Int, iterations: Int, lambda: Double, rmse: Double)

object OfflineRecommender {
  // 定义常量，如路径，mongoDB中存储的表名等
  val OPTED_PARAMS = "OptedParams"
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20  /*top20 for recs return*/

  def main(args: Array[String]): Unit = {

    // 配置选项
    val config = Map(
      // TODO: 如何远程连接hadoop集群使用 yarn 模式？？ 或者 如何打包提交以 yarn 模式运行？？
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建一个 spark session  (-->sql)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 操作DF DS需要引入 implicits
    import spark.implicits._
    // 通过样例类配置 MongoDB 隐式配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载数据 (model.train(RDD), 因此需要加载为 RDD)
    val ratingRDD: RDD[(Int, Int, Double)] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating] /*Dataset*/
      .rdd /*DataSet --> RDD: DataSet.rdd 即可*/
      .map(
        // TODO: 去掉timestamp 转化为 userId: {product1:score1, product2:score2, ...} 格式的数据, 即最后存储进 mongodb 的格式
        rating => (rating.userId, rating.productId, rating.score)
      )
      .cache()  /*缓存：级别 MEMORY_ONLY, 序列化的形式缓存在 JVM 的堆空间中*/

    // 提取出所有 user 和 product 的数据集
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    val productRDD: RDD[Int] = ratingRDD.map(_._2).distinct()

    // >>>>>>>>>>>>>>>>>>> 核心算法过程 <<<<<<<<<<<<<<<<<<<<
    // 1. 训练隐语义模型 Rating: org.apache.spark.mllib.recommendation.Rating
    // 将训练集转化为 mllib 模型训练中要求的的 Rating 类型（也可以直接在上一步家在数据是直接转化，见参数优化中做法）
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // TODO: 定义初始化模型训练的参数 rank:隐特征个数, iterations:迭代次数, lambda: 正则化系数, 需要参数调优
    // 从数据库读取最优参数
    val optedParamsDf: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", OPTED_PARAMS)
      .format("com.mongodb.spark.sql")
      .load()
    val optedParamsRdd: RDD[(Int, Int, Double)] = optedParamsDf
      .as[OptedParams]
      .rdd
      .map(
        params => (params.impFeatureNum, params.iterations, params.lambda)
      )

    val params: (Int, Int, Double) = optedParamsRdd.collect()(0)
    println(">>>>>>>>> optimizated params:" + params)
    val (rank, iterations, lambda) = params
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    // 2. 获得预测评分矩阵，得到用户的推荐列表
    // 用userRDD和product做一个笛卡尔积，得到空的 userProductsRDD
    val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productRDD)
    // 对空的 userProducts 进行模型预测
    val predictRating: RDD[Rating] = model.predict(userProducts)
    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs: DataFrame = predictRating.filter(_.rating > 0)
      .map(
        rating => (rating.user, (rating.product, rating.rating)) /*转化为用户推荐列表的格式*/
      )
      .groupByKey() /*按照rating.user聚合*/
      .map {
        case (userId, recs) => /*map中使用case 用作模式匹配*/
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
          /*参数2：按 rating.rating 降序排列，并取top num, 排序后转化为 Recommendation 样例类*/
//        case _ => ("_product", "_rating", "_")  /*case _ 表示所有的情况都考虑完全 否则容易出错*/
      }
      .toDF()
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. 利用每一个商品的特征向量(商品id, 特征值)，计算商品的相似度列表
    // TODO: DoubleMatrix做笛卡尔积方便
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map {
      case (productId, features) => (productId, new DoubleMatrix(features)) /*DoubleMatrix(array): 将array转换成matrix,方便做矩阵运算*/
    }
    // 两两配对商品(笛卡尔积)，计算余弦相似度
    val productRecsCart: RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))] = productFeatures.cartesian(productFeatures)
    val productRecs: DataFrame = productRecsCart
      .filter {
        /*自己与自己做笛卡尔积后,去除掉自己与自己的相似度数据(值最大)*/
        case (a, b) => a._1 != b._1
      }
      .map {
        /*计算余弦相似度*/
        case (a, b) =>
          val simScore = cosineSim(a._2, b._2)
          (a._1, (b._1, simScore)) /*返回每一个 用户相似度列表*/
      }
      .filter(_._2._2 > 0.5)
      .groupByKey()
      .map {
        case (productId, recs) =>
          // 转化为样例类 ProductRecs 对应的格式
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }

  /**
   * 余弦相似度计算方法
   * @param product1
   * @param product2
   * @return
   */
  def cosineSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    // matrix.norm2: L2范数
    product1.dot(product2) / (product1.norm2() * product2.norm2())
  }

}
