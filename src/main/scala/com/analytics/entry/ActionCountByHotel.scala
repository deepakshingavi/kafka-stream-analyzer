package com.analytics.entry

import com.analytics.processors.DataTransformers.{createActionsColumnsByHotel, deriveStartEndTimeFrameColumns, getStreamWriter, getViewClickColumns, loadStreamingDF, resetToSameDate}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, TimestampType}

import scala.util.Try

class ActionCountByHotel {


}

object ActionCountByHotel {
  val logger: Logger = Logger.getLogger(classOf[ActionCountByHotel])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val brokers = Try(args(0))
    if (brokers.isFailure) {
      logger.error("Broker list not provided.")
    }

    val dataTopic = Try(args(1))
    if (brokers.isFailure) {
      logger.error("Hotel data topic name not provided.")
    }
    if (brokers.isSuccess && dataTopic.isSuccess) {
      var df: DataFrame = loadStreamingDF(spark, dataTopic.get, brokers.get)

      df = df.select($"value".cast(StringType), $"timestamp".cast(TimestampType).as("timestamp"))

      df = getViewClickColumns(df, spark)

      df = resetToSameDate(df, spark)

      val dfStreamWriter = getStreamWriter(df)

      dfStreamWriter.foreachBatch((batch: Dataset[Row], id: Long) => {
        logger.info(s"batch id=$id inputRecordCount=${batch.count()} start")

        var batchDf: DataFrame = createActionsColumnsByHotel(batch, spark)
        batchDf = deriveStartEndTimeFrameColumns(batchDf, spark)
        val recordCount = batchDf.count().intValue()
        batchDf.show(numRows = recordCount, truncate = false)

        logger.info(s"batch id=$id outputRecordCount=$recordCount end  ")
      })
        .start().awaitTermination()
    }
  }
}
