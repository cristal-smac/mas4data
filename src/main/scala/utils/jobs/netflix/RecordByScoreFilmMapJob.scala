package utils.jobs.netflix

import utils.jobs.MapJob

/** Map job to have the number of records by score and film on the netflix data set. */
class RecordByScoreFilmMapJob extends MapJob {

  type K = String

  type V = Int

  /** @see utils.jobs.MapJob.map() */
  protected def map(line: String): Iterator[(String, Int)] = {
    val splitLine = line split ","
    val idFilm = splitLine(0)
    val score = splitLine(2)
      Iterator((idFilm+ " " + score, 1))
    }
}
