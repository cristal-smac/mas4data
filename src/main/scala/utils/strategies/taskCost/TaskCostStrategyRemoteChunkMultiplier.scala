package utils.strategies.taskCost
import akka.actor.ActorRef
import utils.tasks.Task

/** Task cost strategy where the remote chunk costs are multiplied by a given
  * multiplier.
  *
  * @param multiplier multiplier used to multiply the cost of a remote chunk
  */
case class TaskCostStrategyRemoteChunkMultiplier(
  multiplier: Int
) extends TaskCostStrategy {

  /** Get the cost of the given task for the given agent.
    *
    * @param task  task for which the cost is asked
    * @param agent agent for which the task cost is asked
    * @return the cost of the task for the agent
    */
  override def getTaskCost(task: Task, agent: ActorRef): Long =
    task.chunks.foldLeft(0.toLong) {
      case (cost, chunk) => if (agent == chunk.owner) {
        cost + chunk.nbValues
      } else {
        cost + chunk.nbValues.toLong * this.multiplier.toLong
      }
    }

}
