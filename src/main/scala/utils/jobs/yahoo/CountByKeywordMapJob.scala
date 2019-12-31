package utils.jobs.yahoo

import utils.jobs.MapJob

/** Map job to have the number of records by temperature on the meteo data set.
  */
class CountByKeywordMapJob extends MapJob {

  type K = String

  type V = Int

  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split "\t"
    val pass = splitLine(3)
    val keywords = pass split " "

    (for (k <- keywords) yield (k, 1)).toIterator
  }

}
