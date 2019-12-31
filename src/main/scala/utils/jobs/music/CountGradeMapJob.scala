package utils.jobs.music

import utils.jobs.MapJob

/** Map job to have the number of grade on the music data set. */
class CountGradeMapJob extends MapJob {

  type K = Int

  type V = Int

  def map(line: String): Iterator[(Int, Int)] = {
    val splitLine = line split "\t"
    val grade = splitLine(2)

    Iterator((grade.toInt, 1))
  }

}
