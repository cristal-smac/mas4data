package utils.jobs.meteo

import utils.jobs.MapJob

/** Map job to have the number of records by temperature and station on the meteo data set. */
class RecordByTemperatureStationMapJob extends MapJob {

  type K = String

  type V = Int

  /** @see utils.jobs.MapJob.map() */
  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split ";"
    val temp = splitLine(7)
    val idStation = splitLine(0)
    var ref = 0.0

    if (temp == "mq" || idStation == "mq") {
      Iterator()
    } else {
      val celcius = temp.toDouble - 273.15
      val floor = celcius.floor

      val ref : Double = (celcius - floor >= 0.5) match {
        case true => floor+0.5
        case false => floor
      }
      Iterator((ref.toString+ " "+ idStation, 1))
    }
  }
}
