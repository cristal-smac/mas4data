package utils.jobs.generated

import utils.jobs.MapJob

/** Map job for generated data. */
class GeneratedDataMapJob extends MapJob {

  type K = String

  type V = (Int, Int)

  protected def map(line: String): Iterator[(String, (Int, Int))] = {
    val splitLine = line split ";"

    // Iterator((splitLine(0) + "_" + splitLine(1) + "_" + splitLine(5), (splitLine(2).toInt, 1)))
    Iterator((splitLine(0) + "_" + splitLine(1), (splitLine(2).toInt, 1)))
  }

}
