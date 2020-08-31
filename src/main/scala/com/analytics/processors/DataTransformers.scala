package com.analytics.processors

import java.nio.file.Files

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataTransformers {

  val CONST_DATE = "2020-01-01 "

  val checkpointTempDir: String = Files.createTempDirectory("temp-dir").toFile.getAbsolutePath

  def loadStreamingDF(spark: SparkSession, topicName:String, brokerList:String): DataFrame = {
    spark.readStream
      .option("startingOffsets", "earliest")
      .option("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .format("kafka")
      .option("subscribe", topicName)
      .option("kafka.bootstrap.servers", brokerList)
      .load()
  }

  /**
   * SPlies the Kafka values into hotelIds and action columns
   *
   * @param df - Kafka data frame
   * @param spark - Spark session
   * @return Dataframe with hotel_id and action column.
   */
  def getViewClickColumns(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("_tmp", split($"value", ","))
      .withColumn("hotel_id", $"_tmp".getItem(0))
      .withColumn("action", $"_tmp".getItem(1))
      .drop("_tmp")
  }

  /**
   * Resets the date from input timestamp and keep the time untouched.
   *
   * @param df - data frame with timestamp column
   * @param spark  - Spark session
   * @return Dataframe with all timestamp with same data `CONST_DATE` value
   */
  def resetToSameDate(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df.withColumn("timestamp", to_timestamp(concat(lit(CONST_DATE),
      substring($"timestamp".as("string"), 12, 19))))
  }

  /**
   * Gets action count for click and view by hotel and timestamp range
   * window - groups the events by 30 minutes
   * pivot - add columns with unique action values and group's add the column to group by clause.
   *
   * @param df
   * @param spark
   * @return
   */
  def createActionsColumnsByHotel(df: DataFrame, spark: SparkSession) : DataFrame = {
    import spark.implicits._
    df.withWatermark("timestamp", "10 seconds ")
      .groupBy($"hotel_id"
        , window($"timestamp", "30 minutes").as("ts_window"))
      .pivot("action", Seq("click", "view"))
      .count()
  }

  /**
   * Splits the window column into start and end time frame column with only time(HH:mm:ss)
   *
   * @param df
   * @param spark
   * @return
   */
  def deriveStartEndTimeFrameColumns(df: DataFrame, spark: SparkSession) : DataFrame = {
    import spark.implicits._
    df.withColumn("start_ts",
      substring($"ts_window".getItem("start").as("string"), 12, 19))
      .withColumn("end_ts",
        substring(($"ts_window".getItem("end") - expr("INTERVAL 1 SECOND")).as("string"), 12, 19))
      .drop("ts_window")
  }

  /**
   * Gets action count for click and view by hotel and timestamp range
   * window - groups the events by 30 minutes
   * pivot - add columns with unique action values and group's add the column to group by clause.
   *
   * @param df
   * @param spark
   * @return
   */
  def createActionsColumnsByLocation(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    df
      //Maximum allowed latency
      .withWatermark("timestamp", "10 seconds ")
      .groupBy($"location"
        //Groups the events
        , window($"timestamp", "30 minutes").as("ts_window"))

      //removes action column and its unique value as columns
      // click and view column
      .pivot("action", Seq("click", "view"))
      .count()
  }

  def getStreamWriter(df: DataFrame): DataStreamWriter[Row] = {
    df.writeStream
      //Spark job will resume from last checkpointing in case if it fails or stopped.
      .option("checkpointLocation", checkpointTempDir)

      //Spark streaming job will process data frame every 5 seconds
      .trigger(Trigger.ProcessingTime(5000))

      //Spark streaming join job support output will be in delta mode and this state can be saved.
      .outputMode(OutputMode.Append())
  }

}
