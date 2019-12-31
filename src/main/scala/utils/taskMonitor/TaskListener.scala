package utils.taskMonitor

import akka.actor.Actor

/**
 * Defines the message classes a TaskListener actor can receive.
 *  Each message takes as an argument the TaskEvent object that wrapped the information on the event.
 * @author JC
 */
object TaskListener {

  case class TaskAdded(event: TaskEvent)

  case class TaskRemoved(event: TaskEvent)

  case class TaskPerformed(event: TaskEvent)

  case class TaskDone(event: TaskEvent)

  case class TaskInitialized(event: TaskEvent)

}

/**
 * A TaskListener actor accepts message corresponding to task event.
 *  It handles them by providing a behavior for each of the possible situation
 *  that generates such an event.
 *
 * @author JC
 *
 */
trait TaskListener extends Actor {

  import TaskListener._

  /**
   * Defines this TaskListener behavior when a task has been removed by a reducer.
   * @param taskEvent the corresponding event
   */
  def taskRemoved(taskEvent: TaskEvent)
  /**
   * Defines this TaskListener behavior when a task has been added  by a reducer.
   * @param taskEvent the corresponding event
   */
  def taskAdded(taskEvent: TaskEvent)
  /**
   * Defines this TaskListener behavior when a task is being performed  by a reducer.
   * @param taskEvent the corresponding event
   */
  def taskPerformed(taskEvent: TaskEvent)
  /**
   * Defines this TaskListener behavior when a task has been done (finished) by a reducer.
   * @param taskEvent the corresponding event
   */
  def taskDone(taskEvent: TaskEvent)
  /**
   * Defines this TaskListener behavior when the task bundle of a reducer is initialized.
   * @param taskEvent the corresponding event
   */
  def taskInitialized(taskEvent: TaskEvent)

  /** Defines reaction to message : it consists in invoking the corresponding method.
   */
  def receive: Receive = {
    case TaskAdded(taskEvent) => {
      this.taskAdded(taskEvent)
    }
    case TaskRemoved(taskEvent) => {
      this.taskRemoved(taskEvent)
    }
    case TaskPerformed(taskEvent) => {
      this.taskPerformed(taskEvent)
    }
    case TaskDone(taskEvent) => {
      this.taskDone(taskEvent)
    }
    case TaskInitialized(taskEvent) => {
      this.taskInitialized(taskEvent)
    }
  }
}
