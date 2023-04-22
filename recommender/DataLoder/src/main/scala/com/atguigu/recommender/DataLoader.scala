package com.atguigu.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @author lx
 * @date 2023/4/19
 */

//样例类

/**
 * 2951,                                                                                    动漫ID， anime_id
 * Gintama: Nanigoto mo Saiyo ga Kanjin nano de Tasho Senobisuru Kurai ga Choudoyoi,        动漫名称，name
 * "Action, Comedy, Historical, Mecha, Parody, Samurai, Sci-Fi, Shounen",                   标签：genre
 * Special,                                                                                 类别：type
 * 1,                                                                                       级数:episodes
 * 8.13,                                                                                    评分：rating
 * 29331                                                                                    评分次数：members
 */
case class Anime(anime_id : Int,
                 name : String,
                 genre : String,
                 anime_type : String,
                 episodes : String,
                 rating :  Double,
                 members : String)

/**
 * 1,     用户id，user_id
 * 226,   动漫id，anime_id
 * -1     该用户对该动漫的评分：rating
 */
case class Rating(user_id : Int,
                  anime_id : Int,
                  rating : Double)

//把mongo和es的配置封装成样例类

/**
 *
 * @param uri MongoDB连接
 * @param db  MongoDB对应数据库
 */
case class MongoConfig(uri:String,
                       db:String)

/**
 *
 * @param httpHosts http主机列表（es），逗号分隔
 * @param transportHosts  transport主机列表，
 * @param index         需要操作的索引
 * @param clustername     集群名称，默认
 */
case class ESConfig(httpHosts:String,
                    transportHosts:String,
                    index:String,
                    clustername:String)

object DataLoader {

//定义常量
  val ANIME_DATA_PATH = "C:\\Users\\lx\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoder\\src\\main\\resources\\anime.csv"
  val RATING_DATA_PATH = "C:\\Users\\lx\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoder\\src\\main\\resources\\rating.csv"

  val MONGODB_ANIME_COLLECTION = "Anime"
  val MONGODB_RATING_COLLECTION = "Rating"
  val ES_ANIME_INDEX = "Anime"

  def main(args: Array[String]): Unit = {

    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "localhost:9200",
      "es.transportHosts" -> "localhost:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "elasticsearch"
    )
//    创建一个SparkConf对象
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

//    创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._


//    加载数据
    val animeRDD = spark.sparkContext.textFile(ANIME_DATA_PATH)

    val animeDF = animeRDD.map(
      item => {
        val attr = item.split(",")
        Anime(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).toDouble,attr(6).trim)
      }
    ).toDF()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble)
    }).toDF()

//    数据预处理

//隐式定
    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

//    将数据保存到MongoDB
    storeDataInMongoDB(animeDF,ratingDF)

//    保存数据到ES
    storeDataInES()

    spark.stop()
  }

  def storeDataInMongoDB(animeDF:DataFrame,ratingDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    //新建一个mongodb的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中已经有相应的数据库，要先进行删除
    mongoClient(mongoConfig.db)(MONGODB_ANIME_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()

    //    将DF数据写入对应的mongodb表中
    animeDF.write
      .option("uri",mongoConfig.uri)
      .option("collection",MONGODB_ANIME_COLLECTION)
      .mode("overwirte")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwirte")
      .format("com.mongodb.spark.sql")
      .save()

    //对数据表建索引
    mongoClient(mongoConfig.db)(MONGODB_ANIME_COLLECTION).createIndex(MongoDBObject("anime_id" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("user_id" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("anime_id" -> 1))

  }
  def storeDataInES():Unit ={

  }
}
