package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  val spark = SparkSession
    .builder()
    .appName("example")
    .config("spark.master", "local")
    .getOrCreate()

  val logger = Logger.getLogger("org.apache.spark")
  logger.setLevel(Level.WARN)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {
    ???
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    ???
  }

  private val stn = StructField("STN", LongType, nullable = false)
  private val wban = StructField("WBAN", LongType, nullable = true)

  private val stationsSchema = StructType(stn :: wban ::
      StructField("lat", DoubleType, nullable = true) ::
      StructField("long", DoubleType, nullable = true) :: Nil
  )

  private val yearSchema = StructType(
    stn :: wban ::
    StructField("month", IntegerType, nullable = false) ::
    StructField("day", IntegerType, nullable = false) ::
    StructField("temperature", IntegerType, nullable = false) :: Nil
  )

  private def read(resource: String, structType: StructType): DataFrame = {
    val rdd = spark.sparkContext.textFile(getResource(resource))
    val lines = rdd
        .map(_.split(",").to[List])
        .map(row)

    spark.createDataFrame(lines, structType)
  }

  private def row(lines: List[String]): Row = Row(lines: _*)

  private def getResource(name: String): String =
    Paths.get(getClass.getResource(name).toURI).toString


}
