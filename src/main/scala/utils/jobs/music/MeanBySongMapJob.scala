package utils.jobs.music

import utils.jobs.MapJob

/** Map job to have the number of records by temperature on the meteo data set.
  */
class MeanBySongMapJob extends MapJob {

  type K = Int

  type V = (Int, Int)

  protected def map(line: String): Iterator[(Int, (Int, Int))] = {
    val splitLine = line split "\t"
    val song = splitLine(0)
    val rate = splitLine(2)

    Iterator((song.toInt, (rate.toInt, 1)))
  }

}
