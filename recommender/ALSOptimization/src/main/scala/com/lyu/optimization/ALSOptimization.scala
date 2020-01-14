package com.lyu.optimization

import breeze.numerics.sqrt
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * @Description:基于ALS算法的协同过滤模型参数优化
 * @ClassName: ALSOptimization
 * @Author: klaus
 * @Date: 2019-12-14 14:11
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

/**
 * MongoDB 连接配置 样例类
 * @param uri   连接uri
 * @param db    要操作的数据库
 */
case class MongoConfig( uri: String, db: String )

/**
 * 定义优化参数样例类
 * @param impFeatureNum 隐特征个数
 * @param iterations 迭代次数
 * @param lambda 正则化参数
 * @param rmse 正则化参数
 */
case class OptedParams( impFeatureNum: Int, iterations: Int, lambda: Double, rmse: Double)

object ALSOptimization {
  val MONGODB_RATING_COLLECTION = "Rating"
  val OPTED_PARAMS_ALS = "OptedParams"

  def main(args: Array[String]): Unit = {
    // 配置选项
    val config = Map(
      // TODO: 如何远程连接hadoop集群使用 yarn 模式？？ 或者 如何打包提交以 yarn 模式运行？？
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个 spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSOptimization")
    // 创建一个 spark session  (-->sql)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 操作DF DS需要引入 implicits
    import spark.implicits._
    // 通过样例类配置 MongoDB 隐式配置对象
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 1. 加载数据 (model.train(RDD), 因此需要加载为 RDD)
    val ratingRDD: RDD[Rating] = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating] /*Dataset*/
      .rdd /*DataSet --> RDD: DataSet.rdd 即可*/
      .map(
        // TODO: 去掉timestamp 转化为 userId: {product1:score1, product2:score2, ...} 格式的数据, 即最后存储进 mongodb 的格式
        // TODO: 将rating转化为mllib模型训练中要求的Rating数据类型  ratingRDD: RDD[Rating]
        rating => Rating(rating.userId, rating.productId, rating.score)
      ).cache()

    // 2. 数据集切分为 训练集 和 测试集
    val splitsData: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.75, 0.25))
    val trainingRDD: RDD[Rating] = splitsData(0)
    val testingRDD: RDD[Rating] = splitsData(1)

    // 3. 核心算法实现：输出最优参数 rank, iterations, lambda (封装成方法)
    val optedParamsArray: Array[(Int, Int, Double, Double)] = optimizeALSparams( trainingRDD, testingRDD )
    val optedParamsRdd: RDD[(Int, Int, Double, Double)] = spark.sparkContext.parallelize(optedParamsArray)

    // 4. rdd --> DF
    val optedParamsDf: DataFrame = optedParamsRdd.map { item =>
      OptedParams(item._1, item._2, item._3, item._4)
    }.toDF

    // 5. DF --> mongodb
    storeDFInMongoDB(optedParamsDf, OPTED_PARAMS_ALS)

    spark.stop()
  }

  /**
   *
   * @param trainData
   * @param testData
   * @return
   */
  def optimizeALSparams(trainData: RDD[Rating], testData: RDD[Rating]): Array[(Int, Int, Double, Double)]= {
    // 定义参数范围
    val rankRange: Array[Int] = Array(5, 10, 20, 50)
    val lambdaRange: Array[Double] = Array(1, 0.1, 0.01)
    val iterationRange: Array[Int] = Array(10, 15)
    // 遍历数组中定义的参数取值并存储每一次循环的 RMSE 结果
    val result: Array[(Int, Int, Double, Double)] = for (rank <- rankRange; iteration <- iterationRange; lambda <- lambdaRange)
      yield {
        val model: MatrixFactorizationModel = ALS.train(trainData, rank, iteration, lambda)
        val rmse: Double = getRMSE(model, testData) /*需自定义函数 getRMSE*/
        // 返回结果
        (rank, iteration, lambda, rmse)
      }
    // 按照 rmse 排序并输出最优参数
    val optedParamsTuple: (Int, Int, Double, Double) = result.minBy(_._4)
    println(optedParamsTuple)   /* (5,10,0.1,1.3277206296878827) */
    // 输出 result数组中的 所有内容
    // result.foreach(println)
    // 最优参数
    val optedParamsArray: Array[(Int, Int, Double, Double)] = Iterator(optedParamsTuple).toArray
    optedParamsArray
  }

  /**
   * 计算均方根误差 rmse
   * @param model
   * @param data
   * @return
   */
  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 构建 userProducts, 得到预测评分矩阵 (针对 测试集，因此 userProducts 矩阵只需用测试集中的 user 和 product 作预测评分矩阵)
    // 在 OfflineRecommender 中因为是需要所有用户和产品,因此是采用 userRDD 和 ProductRDD 做笛卡尔积获得空的预测评分矩阵
    val userProducts: RDD[(Int, Int)] = data.map(item => (item.user, item.product))
    // 对空的 userProducts 进行模型预测
    val predictRating: RDD[Rating] = model.predict(userProducts)
    // 按照公式计算 rmse
    // 将每一个真实评分对应的数据转化为 ((userId, productId), rating_real), ((userId, productId), rating_pred)
    val observed_y: RDD[((Int, Int), Double)] = data.map(item => ( (item.user, item.product), item.rating) )
    val predicted_y: RDD[((Int, Int), Double)] = predictRating.map(item => ( (item.user, item.product), item.rating) )
    // TODO: 调用 RDD 的 join 方法实现两个 RDD 的连接
    sqrt(
      observed_y.join(predicted_y).map{
        case ((userId, productId), (actual, pred)) =>
          val err = actual - pred
          // 返回 误差的平方
          err * err
      }.mean()
    )
  }

  /**
   * 将DataFrame写入mongodb数据库
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

