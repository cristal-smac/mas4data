package utils.jobs.countkey

import utils.jobs.MapJob

/** Map job for the count key problem. */
class CountKeyMapJob extends MapJob {

  type K = Int

  type V = Int

  protected def map(line: String): Iterator[(Int, Int)] = {
    val splitLine = line split ";"

    Iterator((splitLine(0).toInt, splitLine(1).toInt))
  }

}
