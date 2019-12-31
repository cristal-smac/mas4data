package utils.strategies.taskCost
import akka.actor.ActorRef
import utils.tasks.{Chunk, Task}

/** Task cost strategy which uses hardware measures (debit, hard drive latency,
  * network latency).
  */
case class TaskCostStrategyHardwareMeasures(
  readingDebit: Int,
  writingDebit: Int,
  networkDebit: Int,
  hardDriveLatency: Int,
  nbValuesPerTimeFrameProcess: Int
) extends TaskCostStrategy {

  // Time to read a chunk
  private def readingTime(chunk: Chunk): Long = {
    val readingTime = if (this.readingDebit.toLong != 0) {
      chunk.volume / this.readingDebit.toLong
    } else {
      0
    }

    this.hardDriveLatency.toLong + readingTime
  }

  private def processingTime(chunk: Chunk): Long =
    if (this.nbValuesPerTimeFrameProcess != 0) {
      chunk.volume / this.nbValuesPerTimeFrameProcess.toLong
    } else {
      0
    }

  // Cost of processing a local chunk
  // /!\ The writing of the result is not taking into account /!\
  private def localCost(chunk: Chunk): Long = {
    // val resultWritingTime =
    //   this.hardDriveLatency + resultVolume / this.writingDebit

    this.readingTime(chunk) + this.processingTime(chunk) // + resultWritingTime
  }

  // Cost of processing a remote chunk
  private def remoteCost(chunk: Chunk): Long = {
    val transferTime = if (this.networkDebit != 0) {
      chunk.volume / this.networkDebit.toLong
    } else {
      0
    }


    this.readingTime(chunk) + transferTime + this.processingTime(chunk)
  }

  /** Get the cost of the given task for the given agent.
    *
    * @param task  task for which the cost is asked
    * @param agent agent for which the task cost is asked
    * @return the cost of the task for the agent
    */
  override def getTaskCost(task: Task, agent: ActorRef): Long =
    task.chunks.foldLeft(0.toLong) {
      case (cost, chunk) =>
        val chunkCost = if (agent == chunk.owner) {
          this.localCost(chunk)
        } else {
          this.remoteCost(chunk)
        }

        cost + chunkCost
    }

}
