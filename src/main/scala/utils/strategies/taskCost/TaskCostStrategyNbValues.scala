package utils.strategies.taskCost

import akka.actor.ActorRef
import utils.tasks.Task

/** Task cost strategy where the cost of a task equals the number of values to
  * process to get the result.
  */
case object TaskCostStrategyNbValues extends TaskCostStrategy {

  /** Get the cost of the given task for the given agent.
    *
    * @param task  task for which the cost is asked
    * @param agent agent for which the task cost is asked
    * @return the cost of the task for the agent
    */
  override def getTaskCost(task: Task, agent: ActorRef): Long =
    task.chunks.foldLeft(0.toLong) {
      case (cost, chunk) => cost + chunk.nbValues
    }

}
