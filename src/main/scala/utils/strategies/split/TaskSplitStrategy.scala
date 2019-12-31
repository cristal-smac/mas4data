package utils.strategies.split

import akka.actor.ActorRef
import utils.config.ConfigurationBuilder
import utils.tasks.{Chunk, IrTask, Task}

/** Define a task split strategy for the manager. */
sealed trait TaskSplitStrategy {

  /** Define how the manager split tasks.
    *
    * @param ownContribution contribution of the manager
    * @param contributionMap contribution map of the manager
    * @param taskBundle      task bundle of the manager
    * @param owner           owner of the task bundle
    * @return the possible key for which the manager has to consider itself as
    *         final performer and the possible new task bundle of the manager
    *         which contains the split tasks
    */
  def splitTask(
    ownContribution: Long,
    contributionMap: Map[ActorRef, Long],
    taskBundle: List[Task],
    owner: ActorRef
  ): (Option[String], Option[List[Task]])

  /** Build a list of tasks from a list of chunks.
    *
    * @param taskToSplit task to split
    * @param chunksList  chunks list to build tasks from
    * @param taskBundle  task bundle from which come from the task to split
    * @param owner       owner of the task bundle
    * @return the key of the split task and the new task bundle (with the
    *         created sub-tasks)
    */
  protected def chunksToTasks(
    taskToSplit: Task,
    chunksList: List[List[Chunk]],
    taskBundle: List[Task],
    owner: ActorRef
  ): (Option[String], Option[List[Task]]) = {
    val (maybeKey, tasksToAdd) = taskToSplit match {
      case irt: IrTask => (
        None,
        chunksList map {
          chunks => new IrTask(irt.key, chunks, irt.frCost, irt.finalPerformer)
        }
      )

      case _ => (
        Some(taskToSplit.key),
        chunksList map {
          chunks => new IrTask(taskToSplit.key, chunks, taskToSplit.nbValues, owner)
        }
      )
    }

    (maybeKey, Some(tasksToAdd ++ taskBundle.init))
  }

}

/** K task split strategy. */
object KTaskSplitStrategy extends TaskSplitStrategy {

  private lazy val config = ConfigurationBuilder.config

  // Find delta and k
  private def findDeltaAndK(
    ownContribution: Long,
    contributionMap: Map[ActorRef, Long],
    taskCost: Long
  ): Option[(Long, Int)] = {
    val reducersInContributionOrder = contributionMap.toList filter {
      case (_, c)             => c <= ownContribution
    } sortWith {
      case ((_, c1), (_, c2)) => c1 < c2
    }
    val deltas = reducersInContributionOrder map {
      case (_, c) => ownContribution - c
    } filter {
      delta  =>
        (delta <= taskCost) &&
        (delta >= 2 * this.config.chunkSize)
    }

    if (deltas.isEmpty) {
      None
    } else {
      Some(
        (deltas zip (Stream from 1)) minBy {
          case (delta, k) => ownContribution - ((k * delta).toDouble / (k + 1))
        }
      )
    }
  }

  // Divide chunks in k + 1 sub-tasks
  private def divideChunksAmong(
    task: Task,
    k: Int,
    delta: Long
  ): List[List[Chunk]] = {
    def f(
      chunks: List[Chunk],
      chunksCost: Long,
      res: List[(List[Chunk], Long)]
    ): List[List[Chunk]] = {
      val chunkToAdd = chunks.head

      if (chunksCost + chunkToAdd.nbValues >= delta) {
        chunks :: (res map { _._1 } filterNot { _.isEmpty })
      } else {
        val (chunkList, cost) = res.head
        val coupleToAdd = (chunkToAdd :: chunkList, cost + chunkToAdd.nbValues)
        val newRes = (res.tail :+ coupleToAdd) sortWith { _._2 < _._2 }

        f(chunks.tail, chunksCost + chunkToAdd.nbValues, newRes)
      }
    }

    val emptyRes = List.fill(k)((List[Chunk](), 0.toLong))

    f(task.chunks, 0, emptyRes)
  }

  // Determine if a task can be split
  private def canSplitTask(task: Task): Boolean =
    task.nbValues >= 2 * this.config.chunkSize

  /* /!\ /!\ /!\
   * The task bundle is supposed non empty and sorted by increasing order.
   * /!\ /!\ /!\
   */
  def splitTask(
    ownContribution: Long,
    contributionMap: Map[ActorRef, Long],
    taskBundle: List[Task],
    owner: ActorRef
  ): (Option[String], Option[List[Task]]) = {
    val taskToSplit = taskBundle.last

    if (this canSplitTask taskToSplit) {
      val maybeDeltaAndK = this.findDeltaAndK(
        ownContribution,
        contributionMap,
        taskToSplit.nbValues
      )

      if (maybeDeltaAndK.isDefined) {
        val (delta, k) = maybeDeltaAndK.get
        val chunks = this.divideChunksAmong(taskToSplit, k, delta)

        this.chunksToTasks(taskToSplit, chunks, taskBundle, owner)
      } else {
        (None, None)
      }
    } else {
      (None, None)
    }
  }

}

object NaiveTaskSplitStrategy extends TaskSplitStrategy {

  /* /!\ /!\ /!\
   * The task bundle is supposed non empty and sorted by increasing order.
   * /!\ /!\ /!\
   */
  def splitTask(
    ownContribution: Long,
    contributionMap: Map[ActorRef, Long],
    taskBundle: List[Task],
    owner: ActorRef
  ): (Option[String], Option[List[Task]]) = {
    assert(taskBundle.nonEmpty)

    val taskToSplit = taskBundle.last

    if (taskToSplit.chunks.lengthCompare(1) == 0) {
      (None, None)
    } else {
      val l = taskToSplit.chunks
      val n = l.length / 2
      val fstTaskChunks = l take n
      val sndTaskChunks = l drop n

      this.chunksToTasks(
        taskToSplit,
        List(fstTaskChunks, sndTaskChunks),
        taskBundle,
        owner
      )
    }
  }

}
