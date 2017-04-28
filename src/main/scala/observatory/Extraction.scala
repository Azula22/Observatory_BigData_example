package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import Names._
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

  private val stn = StructField(STN, LongType, nullable = false)
  private val wban = StructField(WBAN, LongType, nullable = true)

  private val stationsSchema = StructType(stn :: wban ::
      StructField(Latitude, DoubleType, nullable = true) ::
      StructField(Longitude, DoubleType, nullable = true) :: Nil
  )

  private def stationsRow(line: List[String]): Row = {
    val (stnR, wbanR, latR, longR) = (line.head, line(1), line(2), line(3))
    Row(stnR.toLong, wbanR.toLong, latR.toDouble, longR.toDouble)
  }

  private val yearSchema = StructType(
    stn :: wban ::
    StructField(Month, IntegerType, nullable = false) ::
    StructField(Day, IntegerType, nullable = false) ::
    StructField(Temperature, IntegerType, nullable = false) :: Nil
  )

  private def yearRow(line: List[String]): Row = {
    val (stnR, wbanR, monthR, dayR, tempR) = (line.head, line(1), line(2), line(3), line(4))
    Row(stnR.toLong, wbanR.toLong, monthR.toInt, dayR.toInt, tempR.toInt)
  }

  private def read(resource: String, structType: StructType, rowFunc: List[String] ⇒ Row): DataFrame = {
    val rdd = spark.sparkContext.textFile(getResource(resource))
    val lines = rdd
        .map(_.split(",").to[List])
        .map(rowFunc)

    spark.createDataFrame(lines, structType)
  }

  private def getResource(name: String): String =
    Paths.get(getClass.getResource(name).toURI).toString

}

object Names {
  val STN = "STN"
  val WBAN = "WBAN"
  val Latitude = "latitude"
  val Longitude = "longitude"
  val Month = "month"
  val Day = "day"
  val Temperature = "temperature"
}
