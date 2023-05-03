package com.atguigu.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat

/**
 *
 * @author lx
 * @date 2023/5/3
 */

case class Anime(anime_id : Int,
                 name : String,
                 anime_type : String,
                 episodes : String,
                 rating :  String,
                 members : String)

case class Rating(user_id : Int,
                  anime_id : Int,
                  rating : Double)

case class Tag(anime_id : Int,
               tag : String)

case class MongoConfig(uri:String,
                       db:String)

//定义一个基准推荐对象
case class Recommendation(anime_id:Int,
                          score:Double)

//定义动漫类别推荐统计，top10推荐对象
//类别：Historical   Action  Drama  Mystery  Ecchi    School    Adventure  Hentai  Fantasy  Game
case class GenresRecommendation( genre:String,
                                 recs:Seq[Recommendation])

object StatisticsRecommender {

    //    定义表名
    val MONGODB_ANIME_COLLECTION = "Anime"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"

    //    定义表
    //统计的表的名称
    //    历史评分次数最多
    val RATE_MORE_ANIME = "RateMoreAnime"
    //    最近热门电影统计
    //    val RATE_MORE_RECENTLY_ANIME = "RateMoreRecentlyAnime"
    //    电影平均得分统计
    val AVERAGE_ANIME = "AverageAnime"
    //    每个类别优质电影统计
    val GENRE_TOP_ANIME = "GenreTopAnime"

    // 入口方法
    def main(args: Array[String]): Unit = {
        val config = Map(
            "spark.cores" -> "local[*]",
            "mongo.uri" -> "mongodb://localhost:27017/recommender",
            "mongo.db" -> "recommender"
        )

        //创建 SparkConf 配置
        val sparkConf = new SparkConf().setAppName("StatisticsRecommender").setMaster(config("spark.cores"))

        //创建 SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)
        //加入隐式转换
        import spark.implicits._



        //数据加载进来
        val ratingDF = spark
          .read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_RATING_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[Rating]
          .toDF()

        val animeDF = spark
          .read
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_ANIME_COLLECTION)
          .format("com.mongodb.spark.sql")
          .load()
          .as[Anime]
          .toDF()



        //创建一张名叫 ratings 的表
        ratingDF.createOrReplaceTempView("ratings")
        //TODO: 不同的统计推荐结果

        //        1.历史热门统计：历史评分数据最多  anime_id count
        val rateMoreAnimeDF = spark.sql("select anime_id,count(anime_id) as count from ratings group by anime_id")
        //        把结果写入对应的mongodb表中，
        storeDFInMongoDB(rateMoreAnimeDF, RATE_MORE_ANIME)

//        //        2.近期热门统计，按照时间戳“yyyyMM”格式选取最近的评分数据，统计评分个数
//        //统计以月为单位拟每个电影的评分数
//        //数据结构 -》 anime_id,count,time
//        //创建一个日期格式化工具
//        val simpleDateFormat = new SimpleDateFormat("yyyyMM")
//        //注册一个 UDF 函数，用于将 timestamp 装换成年月格式 1260759144000 => 201605
//        spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x *
//          1000L)).toInt)
//        // 将原来的 Rating 数据集中的时间转换成年月的格式
//        val ratingOfYearMonth = spark.sql("select anime_id, score, changeDate(timestamp) as yearmonth from ratings")
//          // 将新的数据集注册成为一张表 ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")
//        val rateMoreRecentlyAnime = spark.sql("select anime_id, count(anime_id) as count ,yearmonth from ratingOfMonth group by yearmonth, anime_id")
//          rateMoreRecentlyAnime
//          .write
//          .option("uri", mongoConfig.uri)
//          .option("collection", RATE_MORE_RECENTLY_ANIME)
//          .mode("overwrite")
//          .format("com.mongodb.spark.sql")
//          .save()

        //        3.电影平均得分：统计电影的平均评分  anime_id   avg
        val averageAnimeDF = spark.sql("select anime_id , avg(rating) as avg from ratings group by anime_id rating = -1")
//        val averageAnimeDF = spark.sql("select anime_id , avg(rating) as avg from ratings group by anime_id EXCEPT rating = -1")

        storeDFInMongoDB(averageAnimeDF,AVERAGE_ANIME)

        //        4.类别动漫top统计，各个类别统计top10的高评分

//所有的电影类别
        val genres = List("Adventure", "Cars", "Comedy", "Dementia", "Demons", "Drama", "Ecchi", "Fantasy",
            "Game", "Harem", "Hentai", "Historical", "Horror", "Josei", "Kids", "Magic", "Martial Arts", "Mecha",
            "Military", "Music", "Mystery", "Parody", "Police", "Psychological", "Romance", "Samurai",
            "School", "Sci-Fi", "Seinen", "Shoujo", "Shoujo Ai", "Shounen", "Shounen Ai", "Slice of Life",
            "Spacel", "Sports", "Super Power", "Supernatural", "Thriller", "Vampire", "Yaoi", "Yuri", "Action")
        //统计每种电影类型中评分最高的 10 个电影
        //对指定的列空值填充

        val animeNotNullDF = animeDF.na.fill(Map("genre" -> "null"))

        val animeWithScore = animeNotNullDF.join(averageAnimeDF,Seq("anime_id"),"left")

        //将电影类别转换成 RDD
        val genresRDD = spark.sparkContext.makeRDD(genres)

        //计算电影类别 top10
        val genreTopAnimeDF = genresRDD.cartesian(animeWithScore.rdd)
          .filter{
              case (genres,row) =>
                  row.getAs[String]("genre").toLowerCase.contains(genres.toLowerCase)
          }
          .map{
              // 将整个数据集的数据量减小，生成 RDD[String,Iter[mid,avg]]
              case (genres,row) => {
                  (genres,(row.getAs[Int]("anime_id"), row.getAs[Double]("avg")))
              }
          }.groupByKey()
          .map{
              case (genres, items) => GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1,item._2)))
          }.toDF()


        // 输出数据到 MongoDB
        storeDFInMongoDB(genreTopAnimeDF,GENRE_TOP_ANIME)


        spark.stop()


    }

    def storeDFInMongoDB(df:DataFrame,collection_name:String)(implicit mongoConfig: MongoConfig):Unit = {
        df.write
          .option("uri",mongoConfig.uri)
          .option("collection", collection_name)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()
    }
}