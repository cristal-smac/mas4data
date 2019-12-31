package utils.jobs.meteo

import utils.jobs.MapJob

/** Map job to have the mean temperatures by station on the meteo data set. */
class TemperatureByStationMapJob extends MapJob {

  type K = String

  type V = (Double, Int)

  protected def map(line: String): Iterator[(String, (Double, Int))] = {
    val splitLine = line split ";"
    val temp = splitLine(7)

    if (temp == "mq") {
      Iterator()
    } else {
      Iterator((splitLine(0), (temp.toDouble - 273.15, 1)))
    }
  }

}
