package utils.tasks

/** Define an entity which contains chunks. */
trait WithChunks {

  /** Chunks. */
  def chunks: List[Chunk]

  /** Determine if the entity can be divided. */
  def canBeDivided: Boolean

}

/** Define a task which can be divided. */
trait Divisible extends WithChunks {

  val canBeDivided: Boolean = this.chunks.lengthCompare(1) > 0

}

/** Define a task which can not be divided. */
trait NotDivisible extends WithChunks {

  val canBeDivided: Boolean = false

}
