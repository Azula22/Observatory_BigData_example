package observatory

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import ownCode.helpers.Names._

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite with BeforeAndAfter with Matchers {

  private val spark: SparkSession = SparkSession
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

    stationsData = List(
      (49999, Some(6), +72.280, -038.820), //Match
      (3452, None, +72.310,-040.480), //One key
      (49999, Some(7), +72.310, -040.480)//Match
    ).toDF(STN, WBAN, Latitude, Longitude)

    yearData = List(
      (49999, Some(7), 1, 18, 35.3),
      (49999, None, 1, 18, 35.3),
      (3452, None , 1, 26, 22.1),
      (49999, Some(6), 2, 6, 33.2)
    ).toDF(STN, WBAN, Month, Day, Temperature)

    val resolved: Iterable[(LocalDate, Location, Double)] = Extraction.joinAndTransform(stationsData, yearData, year)

    val expectedResult = Iterable(
      (LocalDate.of(year, 2, 6), Location(+72.280, -038.820), Extraction.toCelsius(33.2)),
      (LocalDate.of(year, 1, 18), Location(+72.310, -040.480), Extraction.toCelsius(35.3)),
      (LocalDate.of(year, 1, 26), Location(+72.310, -040.480), Extraction.toCelsius(22.1))
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