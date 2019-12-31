package utils.jobs.meteo

import utils.jobs.MapJob

/** Map job to have the number of records by temperature on the meteo data set.
  */
class RecordByTemperatureMapJob extends MapJob {

  type K = Double

  type V = Int

  protected def map(line: String): Iterator[(Double, Int)] = {
    val splitLine = line split ";"
    val temp = splitLine(7)

    if (temp == "mq") {
      Iterator()
    } else {
      val celcius = temp.toDouble - 273.15
      val floor = celcius.floor

      if (celcius - floor >= 0.5) {
        Iterator((floor + 0.5, 1))
      } else {
        Iterator((floor, 1))
      }
    }
  }

}
