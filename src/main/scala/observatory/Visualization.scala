package observatory

import java.util

import com.sksamuel.scrimage.Image
import Extraction.spark
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.language.implicitConversions

/**
  * 2nd milestone: basic visualization
  */
object Visualization {



  val coef = 2
  val DistanceColumn = "distance"
  val TemperatureColumn = "temperature"
  val LatitudeColumn = "lat"
  val LongitudeColumn = "long"
  val WeightColumn = "weight"
  val WeightWithValueColumn = "wwv"

  //  val colorsMap: Map[Int, Color] = Map(
  //    60 → Color(255, 255, 255),
  //    32 → Color(255, 0, 0),
  //    12 → Color(255, 255, 0),
  //    0 → Color(0, 255, 255),
  //    -15 → Color(0, 0, 255),
  //    -27 → Color(255, 0, 255),
  //    -50 → Color(33, 0, 107),
  //    -60 → Color(0, 0, 0)
  //  )

  import spark.implicits._

  case class LocationWithTemp(lat: Double, long: Double, temp: Double)

  object LocationWithTemp{
    def fromTuple(v: (Location,Double)) = LocationWithTemp(v._1.lat, v._1.lon, v._2)
  }

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {

    def greatCircleFormula: Column = asin(sqrt(
      pow(sin(($"$LatitudeColumn" - location.lat) / 2), 2) +
      cos($"$LatitudeColumn") * Math.cos(location.lat) * pow(sin(($"$LongitudeColumn" - location.lon) / 2), 2)
    )) * 2

    val ds = spark.createDataFrame(temperatures.toSeq)
      .select(
        $"_1.lat".as(s"$LatitudeColumn").cast(DoubleType),
        $"_1.lon".as(s"$LongitudeColumn").cast(DoubleType),
        $"_2".as(s"$TemperatureColumn").cast(DoubleType))
      .withColumn(DistanceColumn, greatCircleFormula)

    val closePoints: util.List[Row] = ds.filter($"$DistanceColumn" < 1)
      .sortWithinPartitions(s"$DistanceColumn")
      .collectAsList()

    if (!closePoints.isEmpty) {
      closePoints.get(0).getAs[Double](s"$DistanceColumn")
    } else {

      val updatedDs = ds
        .withColumn(s"$WeightColumn", lit(1).cast(DoubleType) / pow($"$DistanceColumn", coef))
        .withColumn(s"$WeightWithValueColumn", $"$TemperatureColumn" * $"$WeightColumn")

      val upSum = updatedDs.agg(
        sum(s"$WeightWithValueColumn").cast(DoubleType),
        sum(s"$WeightColumn").cast(DoubleType)
      ).first()

      upSum.getAs[Double](0) / upSum.getAs[Double](1)

    }

  }



  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Double, Color)], value: Double): Color = {

    val pointsMap = points.toMap

    pointsMap.getOrElse(value, {

      val sortedKeys = pointsMap.keys.toVector.sorted

      if (sortedKeys.last < value) pointsMap(sortedKeys.last) else
      if (sortedKeys.head > value) pointsMap(sortedKeys.head)

      else {

        val leftKey: Double = sortedKeys.takeWhile(_ < value).lastOption.getOrElse(sortedKeys.head)
        val rightKey: Double = sortedKeys.collectFirst { case v if v > value ⇒ v }.getOrElse(sortedKeys.last)

        val Length = rightKey - leftKey
        val length = value - leftKey

        if (Length != 0 && length != 0) interp(pointsMap(leftKey), pointsMap(rightKey))(factor = length / Length)
        else pointsMap(value)

      }

    })

  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Double)], colors: Iterable[(Double, Color)]): Image = {
    ???
  }

  private def interp(leftColor: Color, rightColor: Color)(implicit factor: Double): Color = {

    val Color(red1, green1, blue1) = leftColor
    val Color(red2, green2, blue2) = rightColor

    val red3 = countColor(red1, red2)
    val green3 = countColor(green1, green2)
    val blue3 = countColor(blue1, blue2)

    implicit def round(a: Double): Int = Math.round(a).toInt

    Color(red3, green3, blue3)

  }

  private def countColor(v1: Int, v2: Int)(implicit factor: Double) = v1 + ((v2 - v1).toDouble * factor)


}

