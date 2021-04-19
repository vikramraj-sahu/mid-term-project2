import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object VideoBatchIngestion {
  def main(args: Array[String]):Unit = {

    // Video Play Data files
    val videoPlayFilePath = "hdfs://nameservice1/user/edureka_788309/mid-project2/datasets/"
    val companyWebsiteVideoPlayFile = videoPlayFilePath + "video_plays.xml"
    val otherWebsiteVideoPlayFile = videoPlayFilePath + "video_plays.csv"

    // Create spark Session
    val spark = SparkSession.builder
      .appName("Batch Ingestion of Video Play Website Data ")
      .master("yarn")
      .getOrCreate()

    val sc = spark.sparkContext
    val conf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    val xmlFile = fs.exists(new org.apache.hadoop.fs.Path(companyWebsiteVideoPlayFile))

    // Validate the existence of XML File
    if (!xmlFile) {
      println("VIDEO PLAY FILE NOT FOUND OF COMPANY WEBSITE")
      return
    }

    // Validate the existence of CSV File
    val csvFile = fs.exists(new org.apache.hadoop.fs.Path(companyWebsiteVideoPlayFile))
    if (!csvFile) {
      println("VIDEO PLAY FILE NOT FOUND OF OTHER WEBSITE")
      return
    }

    // Create dataframe from XML file
    val companyWebsiteDf: DataFrame = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "record")
      .option("dateFormat", "dd/MM/yyy H:m:s")
      .option("mode", "DROPMALFORMED")
      .load(companyWebsiteVideoPlayFile)

    // Create dataframe from CSV file
    val otherWebsiteDfTemp: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("dateFormat", "dd/MM/yyy H:m:s")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", ",")
      .load(otherWebsiteVideoPlayFile)
    val otherWebsiteDf = otherWebsiteDfTemp.select(col("channel_id").cast("long"), col("creator_id").cast("long"), col("disliked").cast("boolean"), col("geo_cd").cast("string"), col("liked").cast("boolean"), col("minutes_played").cast("long"), col("timestamp").cast("string"), col("user_id").cast("long"), col("video_end_type").cast("long"), col("video_id").cast("long"))

    // Combine the data of XML and CSV
    val videoPlayDf: DataFrame = companyWebsiteDf.union(otherWebsiteDf)

    val channelGeoCdDf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com")
      .option("dbtable", "labuser_database.channel_geocd_788309")
      .option("user", "edu_labuser").option("password", "edureka")
      .load()

    def getGeoCdFromChannelId(channelId: String) = channelGeoCdDf.filter(col("channel_id") === channelId).select(col("geo_cd")).collect().map(_.getString(0)).mkString("")

    // Clean the records
    val cleanVideoPlayDf = videoPlayDf
      .withColumn("liked", when(col("liked") === null, false).otherwise(col("liked")))
      .withColumn("disliked", when(col("disliked") === null, false).otherwise(col("disliked")))
      .withColumn("video_end_type", when(col("video_end_type") =!= 0 && col("video_end_type") =!= 1 && col("video_end_type") =!= 2 && col("video_end_type") =!= 3, 3).otherwise(col("video_end_type")))
      .withColumn("geo_cd", when(col("geo_cd") === null, getGeoCdFromChannelId(col("channel_id").toString())).otherwise(col("geo_cd")))
      .withColumn("geo_cd", when(col("geo_cd") === "", getGeoCdFromChannelId(col("channel_id").toString())).otherwise(col("geo_cd")))

    // Date enrichment
    val enrichedData = cleanVideoPlayDf.filter("(user_id IS NOT NULL AND user_id != 0) AND (video_id IS NOT NULL AND video_id != 0) AND (creator_id IS NOT NULL AND creator_id != 0) AND (timestamp IS NOT NULL AND timestamp != '') AND minutes_played IS NOT NULL AND (geo_cd IS NOT NULL AND geo_cd != '') AND (channel_id IS NOT NULL AND channel_id != 0) AND video_end_type IS NOT NULL AND liked IS NOT NULL AND disLiked IS NOT NULL")

    // Save Enriched data in HDFS
    new Properties()
    enrichedData.select("user_id", "video_id", "creator_id", "timestamp", "minutes_played", "geo_cd", "channel_id", "video_end_type", "liked", "disLiked")
      .write
      .orc("hdfs://nameservice1/user/edureka_788309/mid-project2/datasets/enriched/")
    spark.close()
  }
}