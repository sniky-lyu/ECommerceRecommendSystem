package com.lyu.content

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix


/**
 * 定义 product 样例类 -- 对原始数据进行字段筛选
 *
 * @param productId
 * @param name
 * @param imageUrl
 * @param categories
 * @param tags  商品UGC标签
 */
case class Product( productId: Int, name: String, imageUrl: String, categories: String, tags: String )
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
 * 定义用户的推荐列表 物品两两之间相似度 productId:{productId1:score1, productId2:score2,....}
 * @param productId
 * @param recs
 */
case class ProductRecs( productId: Int,  recs: Seq[Recommendation])

object ContentRecommender {
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {

    val config = Map(
      // TODO: 如何远程连接hadoop集群使用 yarn 模式？？ 或者 如何打包提交以 yarn 模式运行？？
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://192.168.13.128:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentBasedRecommender")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 载入数据 预处理  商品数据 tag
    val productTagsDF: DataFrame = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]  /*Dataset*/
      .map(x => (x.productId, x.name, x.tags.replace("|", " ")))
      .toDF("productId", "name", "tags")  /*DataFrame: ds.toDF(colnames)*/
      .cache()

    // TODO: 用 TF-IDF 提取商品特征
    // 1. 实例化一个分词器，用来做分词, 默认用空格分开
    val tokenizer: Tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器转换，得到增加一个新列 words 的DF
    val wordsDataDF: DataFrame = tokenizer.transform(productTagsDF)
    // 2. 定义一个 HashingTF 工具，计算 词频TF （如果setNumFeatures(默认262144)值不能覆盖所有的特征，则可能出现hash碰撞，即几个特征分在同一个hash桶内）
    val hashingTF: HashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF: DataFrame = hashingTF.transform(wordsDataDF)
//    featurizedDataDF.show(truncate = false)   /*|(300,[38,39,83,104,293],[1.0,1.0,1.0,1.0,1.0])   稀疏向量 1.0表示词出现的频次*/
    // 3. 定义一个 IDF 工具，计算 TF-IDF
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练一个 idf 模型
    val idfModel: IDFModel = idf.fit(featurizedDataDF)
    // 得到增加新列 features 的DF
    val rescaledDataDF: DataFrame = idfModel.transform(featurizedDataDF)
    rescaledDataDF.show(5,false)
    /*|(262144,[104222,181018,183798,240230,243454],[3.8815637979434374,1.2074151485169087,3.188416617383492,2.495269436823547,2.3774864011671637])*/
    /*3.88,1.207,3.188,...  对应的词的 tf-idf 值，即词在文档中的权重*/

    // 对数据转换 得到 RDD 形式的 features (features字段的格式是稀疏向量 SparseVector)
    val productFeatures: RDD[(Int, DoubleMatrix)] = rescaledDataDF.map {
      row => (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
    }
      .rdd
      .map {
        case (productId, features) => (productId, new DoubleMatrix(features))
      }

    // 两两配对商品(笛卡尔积)，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      .map{   /*计算余弦相似度*/
        case (a, b) =>
          val simScore = cosineSim(a._2, b._2)
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", CONTENT_PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
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
