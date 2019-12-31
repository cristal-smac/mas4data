package utils.tasks

import utils.config.ConfigurationBuilder

/** Object which deals with the task creation. */
object TaskBuilder {

  private lazy val config = ConfigurationBuilder.config

  /** Create a new task from a key and a list of chunks.
    *
    * @param key    key of the task
    * @param chunks chunks of the task
    * @return a new task with the given key and chunks
    */
  def buildTask(key: String, chunks: List[Chunk]): Task =
    if (this.config.withTaskSplit) {
      new Task(key, chunks) with Divisible
    } else {
      new Task(key, chunks) with NotDivisible
    }

  /** Merge two tasks.
    *
    * @param t1 first task to merge
    * @param t2 second task to merge
    * @return a new task, result of the merge of t1 with t2
    */
  def mergeTasks(t1: Task, t2: Task): Task = (t1, t2) match {
    case (_: Divisible, _: Divisible)       =>
      assert(t1.key == t2.key)
      new Task(t1.key, t1.chunks ++ t2.chunks) with Divisible

    case (_: NotDivisible, _: NotDivisible) =>
      assert(t1.key == t2.key)
      new Task(t1.key, t1.chunks ++ t2.chunks) with NotDivisible

    case _                                  =>
      throw new IllegalArgumentException(
        "Tasks can not be merged (different key or different nature)"
      )
  }

}
