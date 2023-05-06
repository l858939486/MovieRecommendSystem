package com.atguigu.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 *
 * @author lx
 * @date 2023/5/3
 */
//基于评分数据的LFM，只需要rating数据，mllib中自带rating，进行区分所以明明为AnimeRating
case class Anime(mid : Int,
                 name : String,
                 anime_type : String,
                 episodes : String,
                 rating :  String,
                 members : String)

case class AnimeRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)
// 标准推荐对象，mid,score
case class Recommendation(mid: Int, score:Double)
// 用户推荐
case class UserRecs(uid: Int, recs: Seq[Recommendation])
// 动漫相似度（动漫推荐）
case class AnimeRecs(mid: Int, recs: Seq[Recommendation])
object OfflineRecommmeder {
    // 定义常量
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_ANIME_COLLECTION = "Anime"
    // 推荐表的名称
    val USER_RECS = "UserRecs"
    val ANIME_RECS = "AnimeRecs"
    val USER_MAX_RECOMMENDATION = 20
    def main(args: Array[String]): Unit = {
        // 定义配置
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )
        // 创建 spark session
        val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("Offline")
          .set("spark.executor.memory","4G")
          .set("spark.driver.memory","2G")
          .set("spark.speculation","True")
          .set("spark.speculation.multiplier","2")
          .set("spark.speculation.quantile","0")
          .set("spark.executor.cores","6")

        val spark = SparkSession.builder().config(sparkConf).getOrCreate()
        implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))
        import spark.implicits._


        //读取 mongoDB 中的业务数据
        val ratingRDD = spark
          .read
          .option("uri",mongoConfig.uri)
          .option("collection",MONGODB_RATING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[AnimeRating]
          .rdd
          .map(rating=> (rating.uid, rating.mid, rating.score))
          .cache()
          .repartition(3)
        //加载评分数据

        //用户的数据集 RDD[Int]
        val userRDD = ratingRDD.map(_._1).distinct().repartition(3)
        val animeRDD = ratingRDD.map(_._2).distinct().repartition(3)


////        数据集 RDD[Int]
//        val animeRDD = spark
//          .read
//          .option("uri",mongoConfig.uri)
//          .option("collection",MONGODB_ANIME_COLLECTION)
//          .format("com.mongodb.spark.sql")
//          .load()
//          .as[Anime]
//          .rdd
//          .map(_.mid).cache()

        //创建训练数据集
        val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))
        // rank 是模型中隐语义因子的个数, iterations 是迭代的次数, lambda 是 ALS 的正则化参
        val (rank,iterations,lambda) = (200, 5, 0.01)
        // 调用 ALS 算法训练隐语义模型
        val model = ALS.train(trainData,rank,iterations,lambda)
        //计算用户推荐矩阵
        val userAnimes = userRDD.cartesian(animeRDD)
        // model 已训练好，把 id 传进去就可以得到预测评分列表 RDD[Rating] (uid,mid,rating)
        val preRatings = model.predict(userAnimes)


        val userRecs = preRatings
          .filter(x => x.rating > 0)
          .map(rating => (rating.user,(rating.product, rating.rating)))
          .groupByKey()
          .map{
              case (uid,recs) => UserRecs(uid,recs.toList.sortWith(_._2 > _._2)
                .take(USER_MAX_RECOMMENDATION)
              .map(x => Recommendation(x._1,x._2)))
          }
          .toDF()



        userRecs.write
          .option("uri",mongoConfig.uri)
          .option("collection",USER_RECS)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

//


////        基于动漫的特征方程，计算相似度矩阵，得到电影的相似度列表
//        val animeFeatures = model.productFeatures.map{
//            case(mid,features) => (mid,new DoubleMatrix(features))
//        }
//
////        对所有动漫两两计算他们的相似度，先做笛卡尔积
//        val animeRecs = animeFeatures.cartesian(animeFeatures)
//          .filter{
//              case(a,b) => a._1 != b._1
//          }
//          .map{
////                      将动漫1 与 动漫2 相似度组合
//              case (a,b) =>{
//                  val simScore = this.consinSim(a._2,b._2)
//                  ( a._1, ( b._1,simScore )  )
//              }
//          }
////          筛选相似度高于0.6的
//          .filter(_._2._2 > 0.6)
//          .groupByKey()
//          .map{
//              case (mid ,items) => AnimeRecs (mid,items.toList.sortWith(_._2 > _._2)
//              .map(x => Recommendation(x._1,x._2)))
//          }
//          .toDF()
//
//        animeRecs.write
//          .option("uri", mongoConfig.uri)
//          .option("collection", ANIME_RECS)
//          .mode("overwrite")
//          .format("com.mongodb.spark.sql")
//          .save()

        spark.stop()
    }

//    求向量余弦相似度
    def consinSim(anime1: DoubleMatrix, anime2: DoubleMatrix):Double = {
        anime1.dot(anime2) / ( anime1.norm2() * anime2.norm2() )
    }

}
