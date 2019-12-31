package utils.jobs.yahoo

import utils.jobs.MapJob

/** Map job to have the number of records by temperature on the meteo data set.
  */
class CountByAccountMapJob extends MapJob {

  type K = String

  type V = Int

  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split "\t"
    val account = splitLine(1)
    Iterator((account, 1))
  }

}
