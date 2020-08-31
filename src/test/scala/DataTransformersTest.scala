import com.analytics.processors.DataTransformers._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class DataTransformersTest extends AnyFunSuite
  with BeforeAndAfter {
  self =>

  var spark: SparkSession = _
  var baseDf: DataFrame = _

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }

  import testImplicits._

  before {
    spark = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()
  }

  after {
    spark.close()
  }

  /**
   * Input schema
   * |-- value: string (nullable = true)
   *
   * Output schema
   * |-- value: string (nullable = true)
   * |-- hotel_id: string (nullable = true)
   * |-- action: string (nullable = true)
   */
  test("Test column value column split to hotel_id and action") {
    val testDf = Seq("9089,view", "6754,view", "1437,view", "8876,click ", "9089,click").toDF("value")
    val result = getViewClickColumns(testDf, spark)
    assertResult(Array("value", "hotel_id", "action"))(result.columns)
  }

  /**
   * Input schema
   * |-- timestamp: timestamp (nullable = true)
   *
   * Output Schema
   * |-- timestamp: timestamp (nullable = true)
   */
  test(s"Reset date from timestamp column") {
    val testDf = Seq("2021-05-04 05:32:45", "1990-01-01 23:32:35").toDF("timestamp").withColumn("timestamp", $"timestamp".cast(TimestampType))
    val result = resetToSameDate(testDf, spark)
    val dates = result.select("timestamp").as[String].collect()
    assertResult(Array("2020-01-01 05:32:45", "2020-01-01 23:32:35"))(dates)
  }

  /**
   * Input schema
   * |-- hotel_id: string (nullable = true)
   * |-- action: string (nullable = true)
   * |-- timestamp: string (nullable = true)
   *
   * Output schema
   * root
   * |-- hotel_id: string (nullable = true)
   * |-- ts_window: struct (nullable = false)
   * |    |-- start: timestamp (nullable = true)
   * |    |-- end: timestamp (nullable = true)
   * |-- click: long (nullable = true)
   * |-- view: long (nullable = true)
   */
  test("Add columns for possible action values") {
    val testDf = Seq(("9091", "view", "2020-01-01 05:32:20"), ("9091", "click", "2020-01-01 05:32:20")).toDF("hotel_id", "action", "timestamp")
    val result = createActionsColumnsByHotel(testDf, spark)
    assertResult(Array("hotel_id", "ts_window", "click", "view"))(result.columns)
    val resultData = result.as[(String, (String, String), Long, Long)].collect()
    assertResult(Array(("9091", ("2020-01-01 05:30:00", "2020-01-01 06:00:00"), 1, 1)))(resultData)
  }


  /**
   * Input schema
   * |-- hotel_id: string (nullable = true)
   * |-- ts_window: struct (nullable = false)
   * |    |-- start: timestamp (nullable = true)
   * |    |-- end: timestamp (nullable = true)
   * |-- click: long (nullable = true)
   * |-- view: long (nullable = true)
   *
   * Output Schema
   * |-- hotel_id: string (nullable = true)
   * |-- click: integer (nullable = false)
   * |-- view: integer (nullable = false)
   * |-- start_ts: string (nullable = true)
   * |-- end_ts: string (nullable = true)
   */
  test("Extracting start and end columns") {
    val test = Seq(("6035", Map("start" -> "2020-01-01 00:30:00", "end" -> "2020-01-01 01:00:00"), 5, 1), ("9091", Map("start" -> "2020-01-01 05:30:00",
      "end" -> "2020-01-01 06:00:00"), 1, 1))
      .toDF("hotel_id", "ts_window", "click", "view")
    val result = deriveStartEndTimeFrameColumns(test, spark)
    assertResult(Array("hotel_id", "click", "view", "start_ts", "end_ts"))(result.columns)
    val resultData = result.as[(String, Long, Long, String, String)].collect()
    assertResult(Array(("6035", 5, 1, "00:30:00", "00:59:59"), ("9091", 1, 1, "05:30:00", "05:59:59")))(resultData)
  }

  /**
   * Input schema
   * |-- hotel_id: string (nullable = true)
   * |-- action: string (nullable = true)
   * |-- timestamp: string (nullable = true)
   * |-- location: string (nullable = true)
   *
   * Output schema
   * root
   * |-- hotel_id: string (nullable = true)
   * |-- ts_window: struct (nullable = false)
   * |    |-- start: timestamp (nullable = true)
   * |    |-- end: timestamp (nullable = true)
   * |-- click: long (nullable = true)
   * |-- view: long (nullable = true)
   */
  test("Add columns for possible action values with hotel location") {
    val testDf = Seq(("9091", "view", "2020-01-01 05:32:20","Delhi"), ("9091", "click", "2020-01-01 05:32:20","Mexico City")).toDF("hotel_id", "action", "timestamp","location")
    val result = createActionsColumnsByLocation(testDf, spark)
    assertResult(Array("location", "ts_window", "click", "view"))(result.columns)
    val resultData = result.as[(String, (String, String), Option[Long], Option[Long])].collect()
    assertResult(Array(("Mexico City",("2020-01-01 05:30:00","2020-01-01 06:00:00"),Some(1),None),
      ("Delhi",("2020-01-01 05:30:00","2020-01-01 06:00:00"),None,Some(1))))(resultData)
  }

  case class TsWindow(start: String, end: String)

}
