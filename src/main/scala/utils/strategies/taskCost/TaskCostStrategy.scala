package utils.strategies.taskCost

import akka.actor.ActorRef
import utils.tasks.Task

/** Strategy which determines the cost of a task. */
abstract class TaskCostStrategy {

  /** Get the cost of the given task for the given agent.
    *
    * @param task  task for which the cost is asked
    * @param agent agent for which the task cost is asked
    * @return the cost of the task for the agent
    */
  def getTaskCost(task: Task, agent: ActorRef): Long

}
