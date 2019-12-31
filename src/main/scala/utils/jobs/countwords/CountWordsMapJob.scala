package utils.jobs.countwords

import utils.jobs.MapJob

/** Map job for the count words problem. */
class CountWordsMapJob extends MapJob {

  type K = String

  type V = Int

  /** @see utils.jobs.MapJob.map() */
  protected def map(line: String): Iterator[(String, Int)] = {
    val words = (line split " ") filterNot { _.isEmpty }

    words.foldLeft(Iterator[(String, Int)]()) {
      case (it, word) => it ++ Iterator((word, 1))
    }
  }

}
