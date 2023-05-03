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
case class AnimeRating(user_id : Int,
                  anime_id : Int,
                  rating : Float)

case class MongoConfig(uri:String,
                       db:String)

//定义一个基准推荐对象
case class Recommendation(anime_id:Int,
                          score:Double)

//定义基于预测评分的用户推荐列表
case class UserRecs( user_id:Int,
                     recs:Seq[Recommendation])

//定义基于LFN电影特征向量的电影相似度列表
case class AnimeRecs(anime_id:Int,
                     recs:Seq[Recommendation])

object OfflineRecommender {

    //    定义表名
    val MONGODB_RATING_COLLECTION = "Rating"

    val USER_RECS = "UserRecs"
    val ANIME_RECS = "AnimeRecs"
//user最多20个相似推荐
    val USER_MAX_RECOMMENDATION = 20

    // 入口方法
    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //    创建一个SparkConf对象
        val sparkConf = new SparkConf().setAppName("OfflineRecommender").setMaster(config("spark.cores"))

        //创建 SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get, config.get("mongo.db").get)
        //加入隐式转换
        import spark.implicits._

//        加载数据

        //数据加载进来
        val ratingRDD = spark
          .read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_RATING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[AnimeRating]
          .rdd
          .map(rating => (rating.user_id, rating.anime_id, rating.rating)).cache()

//        从rating数据中提取所有的user_id,anime_id,并且去重
        val userRDD = ratingRDD.map(_._1).distinct()
        val animeRDD = ratingRDD.map(_._2).distinct()

//        训练隐语义模型
        val trainData = ratingRDD.map(x => Rating(x._1,x._2,x._3))


        val (rank,iterations,lambda) = (50,5,0.01)
        // 调用 ALS 算法训练隐语义模型
        val model = ALS.train(trainData,rank,iterations,lambda)
        //        基于用户和电影的特征方程，计算预测评分，得到用户的推荐列表

        //计算用户推荐矩阵
        val userAnime = userRDD.cartesian(animeRDD)
        // model 已训练好，把 id 传进去就可以得到预测评分列表 RDD[Rating] (uid,mid,rating)
        val preRatings = model.predict(userAnime)
        val userRecs = preRatings
          .filter(_.rating > 0)
          .map(rating => (rating.user,(rating.product, rating.rating)))
          .groupByKey()
          .map{
              case (user_id,recs) => UserRecs(user_id,recs.toList.sortWith(_._2 > _._2)
                .take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1,x._2)))
          }.toDF()
        userRecs.write
          .option("uri",mongoConfig.uri)
          .option("collection",USER_RECS)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
        //TODO：计算电影相似度矩阵



//        基于电影的特征方程，计算相似度矩阵，得到电影的相似度列表
        val animeFeatures = model.productFeatures.map{
            case(anime_id,features) => (anime_id,new DoubleMatrix(features))
        }

//        对所有动漫两两计算他们的相似度，先做笛卡尔积
        val animeRecs = animeFeatures.cartesian(animeFeatures)
          .filter{
              case(a,b) => a._1 != b._1
          }
          .map{
//                      将动漫1 与 动漫2 相似度组合
              case (a,b) =>{
                  val simScore = this.consinSim(a._2,b._2)
                  ( a._1, ( b._1,simScore )  )
              }
          }
//          筛选相似度高于0.6的
          .filter(_._2._2 > 0.6)
          .groupByKey()
          .map{
              case (anime_id ,items) => AnimeRecs (anime_id,items.toList.sortWith(_._2 > _._2)
              .map(x => Recommendation(x._1,x._2)))
          }
          .toDF()

        animeRecs.write
          .option("uri", mongoConfig.uri)
          .option("collection", ANIME_RECS)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        spark.stop()
    }

//    求向量余弦相似度
    def consinSim(anime1: DoubleMatrix, anime2: DoubleMatrix):Double = {
        anime1.dot(anime2) / ( anime1.norm2() * anime2.norm2() )
    }

}
