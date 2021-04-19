import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object VideoStreamIngestion {
  def main(args: Array[String]): Unit = {

    // Read only today's data
    val currentDate = java.time.LocalDate.now
    val filePath = "/mnt/bigdatapgp/edureka_788309/mid-project2/data_generator/out/generate_data/"+currentDate+"/json"

    // Check File Existence
    if( !new java.io.File(filePath).exists) {
      println("Error: File Not exits for " + currentDate)
      return
    }

    // Define Json file Schema
    val jsonSchema = StructType(
        List(
          StructField("liked", DataTypes.BooleanType, nullable=true),
          StructField("user_id", DataTypes.LongType, nullable=true),
          StructField("video_end_type", DataTypes.StringType, nullable=true),
          StructField("minutes_played", DataTypes.LongType, nullable=true),
          StructField("video_id", DataTypes.LongType, nullable=true),
          StructField("geo_cd", DataTypes.StringType, nullable=true),
          StructField("channel_id", DataTypes.LongType, nullable=true),
          StructField("creator_id", DataTypes.LongType, nullable=true),
          StructField("timestamp", DataTypes.StringType, nullable=true),
          StructField("disliked", DataTypes.BooleanType, nullable=true)
        )
    )

    // Create spark session
    val spark = SparkSession.builder().appName("Stream Ingestion of Video Play Website Data").master("local[2]").getOrCreate()

    // Load Json file
    val videoMobileDf = spark.readStream.format("json").schema(jsonSchema).load("file://" + filePath)

    // Get Lookup table
    val channelGeoCdDf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://dbserver.edu.cloudlab.com")
      .option("dbtable", "labuser_database.channel_geocd_788309")
      .option("user", "edu_labuser")
      .option("password", "edureka")
      .load()

    def getGeoCdFromChannelId(channelId: String) = channelGeoCdDf.filter(col("channel_id") === channelId).select(col("geo_cd")).collect().map(_.getString(0)).mkString("")

    // Clean the records
    val cleanVideoPlayDf = videoMobileDf
      .withColumn("liked", when(col("liked") === null, false).otherwise(col("liked")))
      .withColumn("disliked", when(col("disliked") === null, false).otherwise(col("disliked")))
      .withColumn("video_end_type", when(col("video_end_type") =!= 0 && col("video_end_type") =!= 1 && col("video_end_type") =!= 2 && col("video_end_type") =!= 3, 3).otherwise(col("video_end_type")))
      .withColumn("geo_cd", when(col("geo_cd") === null, getGeoCdFromChannelId(col("channel_id").toString())).otherwise(col("geo_cd")))
      .withColumn("geo_cd", when(col("geo_cd") === "", getGeoCdFromChannelId(col("channel_id").toString())).otherwise(col("geo_cd")))

    // Date enrichment
    val enrichedData = cleanVideoPlayDf
      .select(col("user_id").cast("long"), col("video_id").cast("long"), col("creator_id").cast("long"), col("timestamp").cast("string"), col("minutes_played").cast("long"), col("geo_cd"), col("channel_id").cast("long"), col("video_end_type").cast("long"), col("liked").cast("boolean"), col("disLiked").cast("boolean"))
      .filter("(user_id IS NOT NULL AND user_id != 0) AND (video_id IS NOT NULL AND video_id != 0) AND (creator_id IS NOT NULL AND creator_id != 0) AND (timestamp IS NOT NULL AND timestamp != '') AND minutes_played IS NOT NULL AND (geo_cd IS NOT NULL AND geo_cd != '') AND (channel_id IS NOT NULL AND channel_id != 0) AND video_end_type IS NOT NULL AND liked IS NOT NULL AND disLiked IS NOT NULL")

    // Save Enriched Streamed data
    val data = enrichedData
      .writeStream
      .format("orc")
      .option("path", "hdfs://nameservice1/user/edureka_788309/mid-project2/datasets/enriched/")
      .option("checkpointLocation",  "hdfs://nameservice1/user/edureka_788309/mid-project2/datasets/checkPointStream/")
      .start()

    data.awaitTermination()
    spark.close()
  }
}