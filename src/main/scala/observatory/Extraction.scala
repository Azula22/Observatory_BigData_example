package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.types._
import ownCode.helpers.Names._
import ownCode.models.GatheredData
/**
  * 1st milestone: data extraction
  */

object Extraction {

  val spark = SparkSession
    .builder()
    .appName("example")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  val logger = Logger.getLogger("org.apache.spark")
  logger.setLevel(Level.WARN)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Int, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    val stationsDF = spark.read
      .schema(stationsSchema)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .csv(getResource(stationsFile))

    val yearDF = spark.read
      .schema(yearSchema)
      .option("delimiter", ",")
      .option("mode", "FAILFAST")
      .csv(getResource(temperaturesFile))

    val dataView = stationsDF.join(yearDF)
      .filter(
        stationsDF(STN) === yearDF(STN) &&
//          (stationsDF(WBAN).isNotNull && yearDF(WBAN).isNotNull && stationsDF(WBAN) === yearDF(WBAN))
//          .otherwise(true) &&
          yearDF.col(Temperature) =!= 9999.9)
      .as[GatheredData]

    dataView.map { data ⇒
      (LocalDate.of(year, data.month, data.day), Location(data.lat, data.long), toCelsius(data.temperature))
    }.collect()

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
      StructField(Latitude, DoubleType, nullable = false) ::
      StructField(Longitude, DoubleType, nullable = false) :: Nil
  )

  private val yearSchema = StructType(
    stn :: wban ::
    StructField(Month, IntegerType, nullable = false) ::
    StructField(Day, IntegerType, nullable = false) ::
    StructField(Temperature, IntegerType, nullable = false) :: Nil
  )

  private def getResource(name: String): String =
    Paths.get(getClass.getResource(name).toURI).toString

  private def toCelsius(fahrenheit: Double): Double = (fahrenheit - 32) * 5/9

}
