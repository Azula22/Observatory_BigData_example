package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 1st milestone: data extraction
  */

object Extraction {

  case class GatheredData(
     id: String,
     lat: Double,
     lon: Double,
     month: Int,
     day: Int,
     temperature: Double)

  case class ForYearly(year: Int, loc: Location, temp: Double)

  object ForYearly {
    def fromTuple(t: (LocalDate, Location, Double)): ForYearly = new ForYearly(t._1.getYear, t._2, t._3)
  }

  val Id = "id"
  val STN = "stn"
  val WBAN = "wban"
  val Latitude = "lat"
  val Longitude = "lon"
  val Month = "month"
  val Day = "day"
  val Temperature = "temperature"
  val YearTemp = "temp"

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
      .option("mode", "DROPMALFORMED")
      .schema(stationsSchema)
      .csv(getResource(stationsFile))
      .withColumn(Id, when($"$WBAN".isNotNull, array($"$STN", $"$WBAN")).otherwise(array($"$STN")))
      .where($"$Latitude".isNotNull && $"$Longitude".isNotNull)
      .drop(STN, WBAN)

    val yearDF = spark.read
      .option("mode", "FAILFAST")
      .schema(yearSchema)
      .csv(getResource(temperaturesFile))
      .where($"$YearTemp" =!= 9999.9)
      .withColumn(Id, when($"$WBAN".isNotNull, array($"$STN", $"$WBAN")).otherwise(array($"$STN")))
      .withColumn(Temperature, ($"$YearTemp" - 32) / 9 * 5)
      .drop(STN, WBAN, YearTemp)

    joinAndTransform(stationsDF, yearDF, year)

  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Double)]): Iterable[(Location, Double)] = {
    records.par
      .groupBy{case (localDate, location, _) ⇒ localDate.getYear -> location}
      .toSeq
      .map { case((_, location),  data) ⇒
          val allTemperatures = data.map(_._3)
          val averageTemp = allTemperatures.sum / allTemperatures.size
          location -> averageTemp
      }(collection.breakOut)
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
    StructField(YearTemp, IntegerType, nullable = false) :: Nil
  )

  private def getResource(name: String): String =
    Paths.get(getClass.getResource(name).toURI).toString

  def joinAndTransform(stationsDF: DataFrame, yearDF: DataFrame, year: Int):  Iterable[(LocalDate, Location, Double)] = {

    val dataView = stationsDF.join(yearDF, Id).as[GatheredData]

    dataView.map ( data ⇒ (data.month, data.day, data.lat, data.lon, data.temperature))
      .collect()
      .par
      .map{ case (mm, dd, lat, lon, temp) ⇒
      (LocalDate.of(year, mm, dd), Location(lat, lon), temp)}(collection.breakOut)
  }

}
