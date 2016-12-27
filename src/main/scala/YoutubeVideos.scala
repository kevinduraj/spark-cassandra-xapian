import com.datastax.spark.connector._

import java.io._
import java.io.File
import java.io.FileFilter
import java.io.FileInputStream
import java.io.PrintWriter
import java.text.{SimpleDateFormat};
import java.time.format.DateTimeFormatter
import java.time.LocalDate
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.matching.Regex

 /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
object YoutubeVideos {

    var batchFiles = new ListBuffer[BufferedWriter]() 
    val locale = new java.util.Locale("us", "US")
    val formatter = java.text.NumberFormat.getIntegerInstance(locale)

    /*--- define log1 ---*/
    val lnOf2 = scala.math.log(2) // natural log of 2
    def log2(x: Double): Double = scala.math.log(x) / lnOf2

    /*--------------------------------------------------------------*/
    def main(args: Array[String]) {

        val output_prefix   = args(1); println("output_prefix=" + output_prefix)

        /*--- Open 100 ranked batchFiles with append flag set to true, ---*/
        for(io <- 0 to 100) { batchFiles +=  new BufferedWriter(new FileWriter(new File(output_prefix + "/video_" + io +".dat"), true)) }

        if(args(0) == "export_data") {
            export_data();
        } else if(args(0) == "export_data")  {
            export_data();
        }

        /*--- Close ranking batchFiles ---*/
        for(ic <- 0 to 100) { batchFiles(ic).close() }
    }
    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
    val user_defined_function = udf(( 
                published_at:   String, 
                duration:       String,
                scoreLikes:     String,
                scoreViews:     String ) => { 

    /*--------------------- [ Normalize Result ] -----------------------------*/
    val random  = scala.util.Random
    var totalRank = random.nextInt(100)
    "xrank" + totalRank.toInt.toString() + " "

    })

    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
    def export_data() {

        val pattern = new Regex("hashtags=\\w+ \\w+ \\w+")

        val spark = SparkSession.builder().appName("YoutubeVideos").config("spark.some.config.option", "some-value").getOrCreate()
            import spark.implicits._

            //val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" )).load().toDF()
            val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" )).load()
            df1.printSchema()
            df1.createOrReplaceTempView("video")

            val video1 = spark.sql("SELECT  video_id, channel_id, channel_text, channel_title, duration, stats_comments, stats_dislikes, stats_favorite," +
                    " stats_likes, stats_views, topics, topics_relevant, ts_video_published, video_category_id, video_language, " +
                    " video_seconds, video_tags, video_text, video_title FROM video WHERE video_title IS NOT NULL").toDF()

            val video2 = df1.withColumn("hashtags", user_defined_function (
                        col("ts_video_published"), 
                        col("duration"), 
                        col("stats_likes"),
                        col("stats_views")  
                        ))

            video2.map(t =>  
                    "video_id="          + t.getAs[String]("video_id")                                       + "\n" 
                    //"video_title="       + t.getAs[String]("video_text")                                     + "\n" +
                    //"video_text="        + t.getAs[String]("video_text").replaceAll("(\\<|\\>|\\(|\\)|/|\"|=|-|\\\\|\\.\\.\\.|\\p{C})", " ") + "\n"
                    ).collect().map(_.trim).foreach( row => { 

                        var mapFile = 0

                        // hashtags=85 over3years days1124 2c2ee 6429f 68876 0dd8f 0e88c ae363
                        try {
                        val date: Option[String] = pattern.findFirstIn(row) 
                        val rankFile = date.map(_.split(" ").toList)
                        val temp  = rankFile.get(0).substring(14)
                        mapFile   = temp.toInt
                        } catch {  case e: Exception => {  mapFile = 0  }  }


                        /* depense on each video rank, place it to its file category */
                        batchFiles(mapFile).write(row + "\n\n") 
                        batchFiles(mapFile).flush();
                        //batchFiles(r.nextInt(201)).write(row + "\n\n") 
                        })

    }
    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/

}
