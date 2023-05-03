package com.atguigu.recommender

import com.atguigu.recommender.DataLoader.{ANIME_DATA_PATH, MONGODB_ANIME_COLLECTION, MONGODB_RATING_COLLECTION, RATING_DATA_PATH}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress

/**
 *
 * @author lx
 * @date 2023/4/19
 */

//样例类

/**
 * 2951,                                                                                    动漫ID， anime_id
 * Gintama: Nanigoto mo Saiyo ga Kanjin nano de Tasho Senobisuru Kurai ga Choudoyoi,        动漫名称，name
 * Special,                                                                                 类别：type
 * 1,                                                                                       级数:episodes
 * 8.13,                                                                                    评分：rating
 * 29331                                                                                    评分次数：members
 */
case class Anime(anime_id : Int,
                 name : String,
//                 genre : String,
                 anime_type : String,
                 episodes : String,
                 rating :  String,
                 members : String)

/**
 * 1,     用户id，user_id
 * 226,   动漫id，anime_id
 * -1     该用户对该动漫的评分：rating
 */
case class Rating(user_id : Int,
                  anime_id : Int,
                  rating : Double)
/**
 * 32281    动漫id，anime_id
 * Drama    类型：tag
 */
case class Tag(anime_id : Int,
                tag : String)

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
    val TAG_DATA_PATH = "C:\\Users\\lx\\IdeaProjects\\MovieRecommendSystem\\recommender\\DataLoder\\src\\main\\resources\\tag.csv"

    val MONGODB_ANIME_COLLECTION = "Anime"
    val MONGODB_RATING_COLLECTION = "Rating"
    val MONGODB_TAG_COLLECTION = "Tag"

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
        val sparkConf = new
            SparkConf().setAppName("DataLoader").setMaster(config("spark.cores"))

//    创建一个SparkSession
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        // 在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
        import spark.implicits._


        //    加载数据
        val animeRDD = spark.sparkContext.textFile(ANIME_DATA_PATH)


        val animeDF = animeRDD.map(
          item => {
            val attr = item.split(",")
            Anime(attr(0).toInt,attr(1).trim,attr(2).trim,attr(3).trim,attr(4).trim,attr(5).trim)
          }
        ).toDF()

        val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)

        val ratingDF = ratingRDD.map(item => {
          val attr = item.split(",")
          Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble)
        }).toDF()

        val tagRDD = spark.sparkContext.textFile(TAG_DATA_PATH)

        val tagDF = tagRDD.map(item => {
            val attr = item.split(",")
            Tag(attr(0).toInt, attr(1).trim)
        }).toDF()

//隐式定
        implicit val mongoConfig = MongoConfig(config.get("mongo.uri").get,config.get("mongo.db").get)



        //    数据预处理

        //        把Anime对应的tag信息添加进去，加一列  tag1|tag2|tag3
        import org.apache.spark.sql.functions._

        val newGenre = tagDF.groupBy($"anime_id")
          .agg(concat_ws("|",collect_set($"tag")).as("genre"))
          .select("anime_id","genre")

//        对newGenre和anime做join，数据合并在一起
        val animeWithGenreDF = animeDF.join(newGenre,Seq("anime_id"),"left")

//        implicit val esConfig = ESConfig(config.get("es.httpHosts"),
//            config.get("es.transportHosts"),
//            config.get("es.index"),
//            config.get("es.cluster.name"))
        //    将数据保存到MongoDB
        storeDataInMongoDB(animeWithGenreDF, ratingDF, tagDF)

        implicit val esConfig = ESConfig(config.get("es.httpHosts").get,
            config.get("es.transportHosts").get,
            config.get("es.index").get,
            config.get("es.cluster.name").get)



        //    保存数据到ES
        storeDataInES(animeWithGenreDF)

        spark.stop()
    }

    def storeDataInMongoDB(animeDF:DataFrame,ratingDF:DataFrame,tagDF:DataFrame)(implicit mongoConfig: MongoConfig): Unit ={

    //新建一个mongodb的连接
        val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //如果mongodb中已经有相应的数据库，要先进行删除
        mongoClient(mongoConfig.db)(MONGODB_ANIME_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    //    将DF数据写入对应的mongodb表中
        animeDF
          .write
          .option("uri",mongoConfig.uri)
          .option("collection",MONGODB_ANIME_COLLECTION)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        ratingDF
          .write
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_RATING_COLLECTION)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        tagDF
          .write
          .option("uri", mongoConfig.uri)
          .option("collection", MONGODB_TAG_COLLECTION)
          .mode("overwrite")
          .format("com.mongodb.spark.sql")
          .save()

        //对数据表建索引
        mongoClient(mongoConfig.db)(MONGODB_ANIME_COLLECTION).createIndex(MongoDBObject("anime_id" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("user_id" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("anime_id" -> 1))
        mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("anime_id" -> 1))


//    关闭Mongodb
        mongoClient.close()
    }
    def storeDataInES(animeDF:DataFrame)(implicit eSConfig: ESConfig):Unit ={
//     新建es配置
        val settings:Settings = Settings.builder().put("cluster.name",eSConfig.clustername).build()

//       新建一个es客户端
        val esClient = new PreBuiltTransportClient(settings)

        //需要将 TransportHosts 添加到 esClient                                                                                                             中
        val REGEX_HOST_PORT = "(.+):(\\d+)".r

        eSConfig.transportHosts.split(",").foreach {
            case REGEX_HOST_PORT(host: String, port: String) => {
                esClient.addTransportAddress(new
                    InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
            }
        }

        //需要清除掉 ES 中遗留的数据
        if (esClient.admin().indices().exists(new
            IndicesExistsRequest(eSConfig.index)).actionGet().isExists) {
            esClient.admin().indices().delete(new DeleteIndexRequest(eSConfig.index))
        }
        esClient.admin().indices().create(new CreateIndexRequest(eSConfig.index))

        //将数据写入到 ES 中
        animeDF
          .write
          .option("es.nodes", eSConfig.httpHosts)
          .option("es.http.timeout", "100m")
          .option("es.mapping.id", "anime_id")
          .mode("overwrite")
          .format("org.elasticsearch.spark.sql")
          .save(eSConfig.index + "/" + ES_ANIME_INDEX)
    }
}
