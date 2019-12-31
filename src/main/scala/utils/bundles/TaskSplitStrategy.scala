package utils.bundles

import akka.actor.ActorRef
import utils.tasks.{Chunk, IrTask, Task}

import scala.collection.mutable

trait TaskSplitStrategy extends TaskBundle {

  /** Create tasks from chunks which are result of the task split process.
    *
    * @param originalTask the task which has been split
    * @param chunksLists  chunks partition from which create new tasks
    * @return new tasks from the given chunks
    */
  protected def createNewTasksAfterTaskSplitProcess(
    originalTask: Task,
    chunksLists: List[List[Chunk]]
  ): List[Task] = originalTask match {
      case irt: IrTask => chunksLists map {
        chunks => new IrTask(irt.key, chunks, irt.frCost, irt.finalPerformer)
      }

      case _ => chunksLists map {
        chunks => new IrTask(
          originalTask.key,
          chunks,
          originalTask.nbValues,
          this.owner
        )
      }
    }

  /** Create new tasks from a list of chunks (result of the task split process)
    * and add them to the task bundle.
    *
    * @param originalTask the task which has been split
    * @param chunksLists  chunks partition from which create new tasks
    */
  protected def addTasksToBundleFromChunks(
    originalTask: Task,
    chunksLists: List[List[Chunk]]
  ): Unit = {
    val tasksToAdd = this.createNewTasksAfterTaskSplitProcess(
      originalTask,
      chunksLists
    )

    tasksToAdd foreach this.addTask
  }

}

/** The naive task split strategy extract the biggest task of the bundle and
  * create two (almost) identical subtasks from it.
  */
trait NaiveTaskSplitStrategy extends TaskSplitStrategy {

  /** Apply the task split process on the task bundle.
    *
    * @param ownerWorkload current workload of the owner
    * @param workloadMap   map which associates agents and their workload
    * @return the potential key of the split task
    */
  override def applySplitProcess(
    ownerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[String] = {
    val taskToSplit = this.showBiggestTask

    if (taskToSplit.chunks.lengthCompare(1) == 0) {
      None
    } else {
      val chunks = taskToSplit.chunks
      val n = chunks.length / 2
      val fstTaskChunks = chunks take n
      val sndTaskChunks = chunks drop n

      // Extract the biggest task and add the subtasks newly created
      this.extractBiggestTask
      this.addTasksToBundleFromChunks(
        taskToSplit,
        List(fstTaskChunks, sndTaskChunks)
      )
      Some(taskToSplit.key)
    }
  }

}
