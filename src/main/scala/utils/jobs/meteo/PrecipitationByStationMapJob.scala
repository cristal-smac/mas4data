package utils.jobs.meteo

import utils.jobs.MapJob

/** Map job to have the precipitation by station on the meteo data set. */
class PrecipitationByStationMapJob extends MapJob {

  type K = String

  type V = Double

  protected def map(line: String): Iterator[(String, Double)] = {
    def getOrElse(input: String): Double = try {
      input.toDouble
    } catch {
      case _: Exception => 0.0
    }

    val splitLine = line split ";"

    Iterator((splitLine(0), getOrElse(splitLine(40))))
  }

}
