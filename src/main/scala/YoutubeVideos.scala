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

    val TOTAL_INDEXES = 200
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
        for(io <- 0 to TOTAL_INDEXES) { batchFiles +=  new BufferedWriter(new FileWriter(new File(output_prefix + "/video_" + io +".dat"), true)) }

        if(args(0) == "export_data") {
            export_data();
        } else if(args(0) == "export_data")  {
            export_data();
        }

        /*--- Close ranking batchFiles ---*/
        for(ic <- 0 to TOTAL_INDEXES) { batchFiles(ic).close() }
    }

    /*----------------------------------------------------------------------------------------------------------------------------*/
    def squareRoot(a: Int): Int = {
      val sqrt = math.sqrt(a)
      if (sqrt % 1 == 0)
        sqrt.toInt
      else
        0
    }    
    /*----------------------------------------------------------------------------------------------------------------------------*/
    val user_defined_function = udf(( 
                published_at:   String, 
                scoreLikes:     String, 
                scoreViews:     String,
                seconds:        String
                ) => { 

            var resPeriod   = "up3years"
            var period      = 0.0
            var total_rank  = 0
            try {
                val beginDate   = published_at.toString.take(10)                                                                                                                                                                               
                val dateFormat  = new SimpleDateFormat("yyyy-MM-dd");
                val date        = new Date();
                val currentDate = dateFormat.format(date)

                val formatter   = DateTimeFormatter.ofPattern("yyyy-MM-dd")
                val oldDate     = LocalDate.parse(beginDate, formatter)
                val newDate     = LocalDate.parse(currentDate, formatter)
                period          = newDate.toEpochDay() - oldDate.toEpochDay()

                if(period > (365*3))      { resPeriod = "over3years";           }   
                else if(period > (365*2)) { resPeriod = "up3years";             }   
                else if(period > (365))   { resPeriod = "up2years up3years";    }   
                else                      { resPeriod = "up1year up3years";     }   

                if(period <= 30)          { resPeriod = resPeriod + " up1month" }
                if(period <= 14)          { resPeriod = resPeriod + " up2weeks" }
                if(period <= 7)           { resPeriod = resPeriod + " up1week"  }
    
                total_rank = TOTAL_INDEXES - squareRoot(period.toInt + (scoreLikes.toInt/10) + (scoreViews.toInt/10)) 

            } catch { case e: Exception => { resPeriod = "over3years" } } 

            /*--------------------- [ Normalize Result ] -----------------------------*/
            val random  = scala.util.Random
            try {
                if(total_rank <  10) { total_rank = random.nextInt(10)        }
                if(total_rank > 200) { total_rank = 191 + random.nextInt(10)  }
            } catch {  case e: Exception => { total_rank = random.nextInt(10)  }  }

            "xrank" + total_rank.toInt.toString() + " " +  resPeriod + " days" + period.toInt.toString() + " "
    })

    /*---------------------------------------------------------------------------------------------------------------------------------------------------
        root
     00    |-- video_id: string (nullable = true)
     01    |-- caption: string (nullable = true)
     02    |-- channel_id: string (nullable = true)
     03    |-- channel_text: string (nullable = true)
     04    |-- channel_title: string (nullable = true)
     05    |-- definition: string (nullable = true)
     06    |-- dimension: string (nullable = true)
     07    |-- duration: string (nullable = true)
     08    |-- etag: string (nullable = true)
     09    |-- kind: string (nullable = true)
     10    |-- projection: string (nullable = true)
     11    |-- safe_alcohol: boolean (nullable = true)
     12    |-- safe_blocked: string (nullable = true)
     13    |-- safe_game_rating: string (nullable = true)
     14    |-- safe_rating: string (nullable = true)
     15    |-- safe_restricted: boolean (nullable = true)
     16    |-- stats_comments: long (nullable = true)
     17    |-- stats_dislikes: long (nullable = true)
     18    |-- stats_favorite: long (nullable = true)
     19    |-- stats_likes: long (nullable = true)
     20    |-- stats_views: long (nullable = true)
     21    |-- topics: string (nullable = true)
     22    |-- topics_relevant: string (nullable = true)
     23    |-- ts_data_update: timestamp (nullable = true)
     24    |-- ts_stats_update: timestamp (nullable = true)
     25    |-- ts_video_published: timestamp (nullable = true)
     26    |-- video_category_id: string (nullable = true)
     27    |-- video_language: string (nullable = true)
     28    |-- video_license: boolean (nullable = true)
     29    |-- video_seconds: integer (nullable = true)
     30    |-- video_tags: string (nullable = true)
     31    |-- video_text: string (nullable = true)
     32    |-- video_title: string (nullable = true) 
    */
    def export_data() {

        val pattern = new Regex("hashtags=\\w+ \\w+ \\w+")

        val spark = SparkSession.builder().appName("YoutubeVideos").config("spark.some.config.option", "some-value").getOrCreate()
            import spark.implicits._

            //val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" )).load()
            val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" )).load().toDF()
            //val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" ))
            //val df2 = df1.filter(df1.col("video_title").isNotNull())
            df1.printSchema()

/*          df1.createOrReplaceTempView("video")
            val video1 = spark.sql("SELECT video_id, video_title, channel_id, channel_text, channel_title, duration, stats_comments, stats_dislikes, stats_favorite," +
                    " stats_likes, stats_views, topics, topics_relevant, ts_video_published, video_category_id, video_language, " +
                    " video_seconds, video_tags, video_text FROM video WHERE video_title IS NOT NULL LIMIT 100").toDF() */

            val video = df1.withColumn("hashtags", user_defined_function (
                        col("ts_video_published"), 
                        col("stats_likes"),
                        col("stats_views"),  
                        col("video_seconds")
                        ))
            //https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/Row.html
            video.map(t =>  
                    "video_id="         + t.getAs[String]("video_id")                       + "\n" + 
                    "video_title="      + ( if(t.isNullAt(32)) "missing title" else t(32) ) + "\n" +                   
                    "video_text="       + ( if(t.isNullAt(31)) "missing description" else t.getAs[String]("video_text").replaceAll("(\\t|\\R|\\<|\\>|\\(|\\)|/|\"|=|-|\\\\|\\.\\.\\.|\\p{C})", " ") ) + "\n" +
                    "video_tags="       + ( if(t.isNullAt(30)) "missing tags" else t.getAs[String]("video_tags").replaceAll("(\\[|\\])", " ") ) + "\n" +
                    "stats_likes="      + ( if(t.isNullAt(19)) "0" else t(19) ) + "\n" +                   
                    "stats_views="      + ( if(t.isNullAt(20)) "0" else t(20) ) + "\n" +                   
                    "video_seconds="    + ( if(t.isNullAt(29)) "0" else t(29) ) + "\n" +                   
                    "hashtags="         + t.getAs[String]("hashtags")           + "\n" 
                    ).collect().map(_.trim).foreach( row => { 

                        var mapFile = 0

                        // hashtags=85 over3years days1124 2c2ee 6429f 68876 0dd8f 0e88c ae363
                        try {
                        val date: Option[String] = pattern.findFirstIn(row)
                        // DEBUG println(date)

                        val rankFile = date.map(_.split(" ").toList)
                        val temp  = rankFile.get(0).substring(14)
                        mapFile   = temp.toInt
                        } catch {  case e: Exception => {  mapFile = 0  }  }


                        /* depense on each video rank, place it to its file category */
                        batchFiles(mapFile).write(row + "\n\n") 
                        batchFiles(mapFile).flush();
                        //batchFiles(r.nextInt(201)).write(row + "\n\n") 
                        })

        spark.stop()
    }
    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/

}
