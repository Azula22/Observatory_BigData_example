package ownCode.models

case class GatheredData(
   stn: Long,
   wban: Option[Long],
   lat: Double,
   long: Double,
   month: Int,
   day: Int,
   temperature: Double)
