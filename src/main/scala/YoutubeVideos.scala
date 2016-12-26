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

        val output_prefix   = args(3); println("output_prefix=" + output_prefix)

        /*--- Open 100 ranked batchFiles with append flag set to true, ---*/
        for(io <- 0 to 200) { batchFiles +=  new BufferedWriter(new FileWriter(new File(output_prefix + "/video_" + io +".dat"), true)) }

        if(args(0) == "export_data") {
            export_data();
        } else if(args(0) == "export_data")  {
            export_data();
        }

        /*--- Close ranking batchFiles ---*/
        for(ic <- 0 to 200) { batchFiles(ic).close() }
    }
    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
    val user_defined_function = udf(( published_at:   String, 
                duration:       String,
                drugsAlcohol:   String,
                scoreLanguage:  String,
                sexualContent:  String,
                scoreViolence:  String,
                scoreLikes:     String,
                scoreViews:     String ) => { 

            var resPeriod   = "up3years"

            var resDuration = "" 
            var resDrugs    = "" 
            var resLanguage = ""
            var resSex      = ""
            var resViolence = ""
            var resViews    = ""

            var period      = 0.0
            var res30day    = 0
            var totalRank   = 0.0
            var globalRank  = 0.0

            /*------------------------- [ published_at ] -----------------------------*/
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


                if (period > 2555) { totalRank +=  1  }
                else if (period > 2529) { totalRank +=  2  }
                else if (period > 2503) { totalRank +=  3  }
                else if (period > 2478) { totalRank +=  4  }
                else if (period > 2452) { totalRank +=  6  }

            } catch { case e: Exception => { resPeriod = "over3years" } }

    //totalRank  = 200 - (log2(period) * 10)


    /*******************************************************************************************
      X_FILTER_MAP = {'3-years-old-filter': "up3years", 
      "brandsafe-language": "6429f",
      "brandsafe-drugs/alcohol": "2c2ee",
      "brandsafe-violence": "0dd8f",
      "brandsafe-nudity": "68876",
      "10k-lifetime-views": "0e88c",
      "30-seconds-duration": "ae363"
      }
     *******************************************************************************************/

    /*------------------------- [ duration ] --------------------------------*/
    try {
        val b = duration.toDouble
            if(b > 30.0 ) { resDuration = "ae363"  }
        totalRank += log2(b) 

    } catch {  case e: Exception => { resDuration = "ae363" }   }


    /*------------------------- [ likes ] --------------------------------*/
    try {
        val likes = scoreLikes.toDouble

            if (likes < 2) { totalRank +=  1  }
            else if (likes < 3) { totalRank +=  2  }
            else if (likes < 7) { totalRank +=  3  }
            else if (likes < 14) { totalRank +=  4  }
            else if (likes < 23) { totalRank +=  6  }
            else if (likes < 35) { totalRank +=  7  }
            else if (likes < 50) { totalRank +=  8  }
            else if (likes < 67) { totalRank +=  9  }
            else if (likes < 87) { totalRank +=  10  }

    } catch {  case e: Exception => { totalRank += 1 }   }

    /*--------------------------------------------------------------------*/
    /*                           [ VIEWS ]                                */
    /*--------------------------------------------------------------------*/
    try {
        val views = scoreViews.toDouble
            if(views > 9999 ) { resViews = "0e88c" } 

        if (views < 2) { totalRank +=  1  }
        else if (views < 3) { totalRank +=  2  }
        else if (views < 7) { totalRank +=  3  }
        else if (views < 13) { totalRank +=  5  }
        else if (views < 21) { totalRank +=  6  }
        else if (views < 31) { totalRank +=  7  }
        else if (views < 43) { totalRank +=  9  }
        else if (views < 57) { totalRank +=  10  }


    } catch {  case e: Exception => { totalRank += 1 }   }

    /*********************************************************************************************/


    /*--------------------- [ Normalize Result ] -----------------------------*/
    val random  = scala.util.Random
    try {
        if(totalRank <  10) { totalRank = random.nextInt(10)      }
        if(totalRank > 200) { totalRank = 191+ random.nextInt(10) }
    } catch {  case e: Exception => { totalRank = random.nextInt(10)  }  }

    // Return string from function
    //resPeriod + " days" + period.toInt.toString() + " xrank" + totalRank.toInt.toString() + " " + resDrugs    + " " + resLanguage + " " + resSex      + " " + resViolence + " " + resViews    + " " +  resDuration
    "xrank" + totalRank.toInt.toString() + " " +  resPeriod + " days" + period.toInt.toString() + " " + resDrugs    + " " + resLanguage + " " + resSex      + " " + resViolence + " " + resViews    + " " +  resDuration

    })

    /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
    def export_data() {

        val pattern = new Regex("hashtags=\\w+ \\w+ \\w+")

        val spark = SparkSession.builder().appName("YoutubeVideos").config("spark.some.config.option", "some-value").getOrCreate()
            import spark.implicits._

            val df1 = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "video2", "keyspace" -> "youtube" )).load()
            df1.printSchema()
            df1.createOrReplaceTempView("video")

            //val df2 = spark.sql("SELECT video_id, video_title, ts_data_update FROM video WHERE ts_data_update <= '2016-12-04 00:00:00+0000'")
            val video1 = spark.sql("SELECT  video_id, channel_id, channel_text, channel_title, duration, stats_comments, stats_dislikes, stats_favorite," +
                    " stats_likes, stats_views, topics, topics_relevant, ts_video_published, video_category_id, video_language, " +
                    " video_seconds, video_tags, video_text, video_title FROM video WHERE ts_stats_update IS NOT NULL LIMIT 25")

            val video2 = video1.withColumn("hashtags", user_defined_function (
                        col("ts_video_published"), 
                        col("duration"), 
                        col("stats_likes"),
                        col("stats_views")  
                        ))

            video2.map(t =>  
                    "video_id="          + t.getAs[String]("video_id")                                       + "\n" +
                    "video_title="       + t.getAs[String]("video_text")                                     + "\n" +
                    "video_text="        + t.getAs[String]("video_text").replaceAll("(\\<|\\>|\\(|\\)|/|\"|=|-|\\\\|\\.\\.\\.|\\p{C})", " ") + "\n"
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
