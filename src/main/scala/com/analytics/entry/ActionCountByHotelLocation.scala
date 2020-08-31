package com.analytics.entry

import com.analytics.processors.DataTransformers._
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.Try

class ActionCountByHotelLocation{

}

object ActionCountByHotelLocation {

  val logger: Logger = Logger.getLogger(ActionCountByHotelLocation.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val brokers = Try(args(0))
    if (brokers.isFailure) {
      logger.error("Broker list nor provided.")
    }

    val dataTopic = Try(args(1))
    if (brokers.isFailure) {
      logger.error("Hotel data topic name not provided.")
    }

    val locationTopic = Try(args(2))
    if (brokers.isFailure) {
      logger.error("Hotel Location topic name not provided.")
    }
    if (brokers.isSuccess && dataTopic.isSuccess && locationTopic.isSuccess) {
      var actions: DataFrame = loadStreamingDF(spark, dataTopic.get, brokers.get)
      actions = actions.select($"value".cast(StringType), $"timestamp".cast(TimestampType).as("timestamp"))
      actions = getViewClickColumns(actions, spark)
      actions = resetToSameDate(actions, spark)

      val locations: DataFrame = loadStreamingDF(spark, locationTopic.get, brokers.get)
        .select($"key".cast(StringType).as("hotel_id"), $"value".cast(StringType).as("location"))
        .select("hotel_id", "location")


      val dfStreamWriter = getStreamWriter(locations.join(actions, "hotel_id"))

      dfStreamWriter.foreachBatch((batch: Dataset[Row], id: Long) => {
        logger.info(s"batch id=$id inputRecordCount=${batch.count()} start")

        var batchDf: DataFrame = createActionsColumnsByLocation(batch, spark)
        batchDf = deriveStartEndTimeFrameColumns(batchDf, spark)
        val recordCount = batchDf.count().intValue()
        batchDf.show(numRows = recordCount, truncate = false)

        logger.info(s"batch id=$id outputRecordCount=$recordCount end  ")
      })
        .start().awaitTermination()
    }
  }


}
