package utils.jobs.netflix

import utils.jobs.MapJob

/** Map job to have the mean score  by film on the netflix data set. */
class ScoreByFilmMapJob extends MapJob {

  type K = String

  type V = (Double, Int)

  protected def map(line: String): Iterator[(String, (Double, Int))] = {
    val splitLine = line split ","
    val score = splitLine(2)

    if (score == "mq") {
      Iterator()
    } else {
      Iterator((splitLine(0), (score.toDouble, 1)))
    }
  }

}
