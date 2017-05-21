package observatory

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite with BeforeAndAfter with Matchers {

  import Extraction.{Month, Day, Temperature, Latitude, Longitude, Id}

  val spark: SparkSession = SparkSession
    .builder()
    .appName("example")
    .config("spark.master", "local")
    .getOrCreate()

  println("------------Started spark session------------")

  private var stationsData: DataFrame = _
  private var yearData: DataFrame = _

  import spark.implicits._

  test("localTemperatures with good data"){

    val year = 2008

    def toCelsius(temp: Double) = (temp - 32) / 9 * 5

    stationsData = List(
      ("649999", +72.280, -038.820), //Match
      ("3452", +72.310,-040.480), //One key
      ("749999", +72.310, -040.480)//Match
    ).toDF(Id, Latitude, Longitude)

    yearData = List(
      ("749999", 1, 18, toCelsius(35.3)),
      ("49999", 1, 18, toCelsius(35.3)),
      ("3452", 1, 26, toCelsius(22.1)),
      ("649999", 2, 6, toCelsius(33.2))
    ).toDF(Id, Month, Day, Temperature)

    val resolved: Iterable[(LocalDate, Location, Double)] = Extraction.joinAndTransform(stationsData, yearData, year)

    val expectedResult = Iterable(
      (LocalDate.of(year, 2, 6), Location(+72.280, -038.820), toCelsius(33.2)),
      (LocalDate.of(year, 1, 18), Location(+72.310, -040.480), toCelsius(35.3)),
      (LocalDate.of(year, 1, 26), Location(+72.310, -040.480), toCelsius(22.1))
    )

    resolved.toSet.shouldEqual(expectedResult.toSet)

  }


  after {
    if (spark != null) {
      spark.stop()

      println("------------Stopped spark session------------")

    }
  }


}