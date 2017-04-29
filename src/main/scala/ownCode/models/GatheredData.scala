package ownCode.models

case class GatheredData(
   stn: Long,
   wban: Option[Long] = None,
   lat: Double,
   lon: Double,
   month: Int,
   day: Int,
   temperature: Double)
