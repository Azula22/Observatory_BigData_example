package observatory

import com.sksamuel.scrimage.Image


/**
  * 2nd milestone: basic visualization
  */
object Visualization {



  val coef = 2
  val TemperatureColumn = "temperature"
  val ColorColumn = "color"

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

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Double)], location: Location): Double = {
    ???
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

      val mayBeResult = for {
        leftKey ← sortedKeys.takeWhile(_ < value).lastOption
        rightKey ← sortedKeys.collectFirst{ case v if v > value ⇒ v }
      } yield {

        val Color(red1, green1, blue1) = pointsMap(leftKey)
        val Color(red2, green2, blue2) = pointsMap(rightKey)

        val Length = rightKey - leftKey
        val length = value - leftKey

        if (Length != 0 && length != 0) {
          val factor = length / Length
          val red3 = red1 + ((red2 - red1) * factor)
          val green3 = green1 + ((green2 - green1) * factor)
          val blue3 = blue1 + ((blue2 - blue1) * factor )

          Color(red3.toInt, green3.toInt, blue3.toInt)

        } else pointsMap(0)
      }

      mayBeResult.getOrElse(pointsMap(0))

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

}

