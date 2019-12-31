package utils.taskMonitor

import akka.actor.ActorRef
import mapreduce.adaptive.AdaptiveReducer
import utils.tasks._

/**
 * defines the class of message a TaskEventHandler can receive
 *
 * @author JC
 *
 */
object TaskEventHandler {

  /**
   * A task is added to a reducer
   * @param taskList the task bundle of the reducer when the task is added
   * @param task the added task
   */
  @SerialVersionUID(1L)
  case class TaskAdded(
    taskBundle: List[Task],
    task: Task
  ) extends Serializable

  /**
   * A task is removed from a reducer
   * @param taskList the task bundle of the reducer when the task is added
   * @param task the added task
   */
  @SerialVersionUID(1L)
  case class TaskRemoved(
    taskBundle: List[Task],
    task: Task
  ) extends Serializable

  /**
   * A task is given to the worker of the reducer
   * @param task the being performed (given) task
   */
  case class TaskPerformed(task: Task)

  /**
   * the reducer's worker has just done (finished) its task
   */
  case object TaskDone

  /**
   * the reducer has received its initial task bundle
   * @param taskBundle the task bundle given to the reducer
   */
  case class TaskInitialized(taskBundle: List[Task])

  /**
   * a new actor is listening to the reducer, this actor must have the TaskListener trait
   * @param taskListener the added listener
   */
  case class AddListener(taskListener: ActorRef)

  /**
   * an actor is no more listening to the reducer
   * @param taskListener the removed listener
   */
  case class RemoveListener(taskListener: ActorRef)

}

/**
 * adds to an active AdaptiveReducer the ability to handle (emit) task event messages
 *
 * Actors that listen to the emitted message must complies to TaskListener trait.
 * @see TaskListener
 *
 * @author JC
 *
 */
trait TaskEventHandler
extends AdaptiveReducer with akka.actor.Stash with utils.debugs.Debug {

  import TaskEventHandler._

  /**
   * the list of actors, that must complies to TaskListener trait, which listen to task
   *  event message emitted by this reducer
   */
  private var listeners: List[ActorRef] = List()

  // Handle the task event related messages.
  private def handleTaskEvent: Receive = {

    case TaskAdded(taskList, task) =>
      val event = new TaskEvent(self.path.name, taskList, task)
      this.fireTaskAdded(event)

    case TaskRemoved(taskList, task) =>
      val event = new TaskEvent(self.path.name, taskList, task)
      this.fireTaskRemoved(event)

    case TaskPerformed(task) =>
      val event = new TaskEvent(self.path.name, List(), task)
      this.fireTaskPerformed(event)

    case TaskDone =>
      val event = new TaskEvent(self.path.name, List(), null)
      this.fireTaskDone(event)

    case RemoveListener(listener) =>
      this removeListener listener

  }

  // Handle the lister initialization process.
  private def handleListnerInit: Receive = {

    case AddListener(listener) =>
      this addListener listener

  }

  // Handle the task initialization process.
  private def handleTaskInitialized: Receive = {

    case TaskInitialized(taskList) =>
      val event = new TaskEvent(self.path.name, List(), null)
      this.fireTaskInitialized(event)

  }

  /** @see mapreduce.adaptive.AdaptiveReducer.receive() */
  override def receive: Receive =
    this.handleListnerInit orElse super.receive

  /** @see mapreduce.adaptive.AdaptiveReducer.waitReady() */
  override def waitReady(nbReady: Int): Receive =
    this.handleTaskInitialized orElse super.waitReady(nbReady)

  /** @see mapreduce.adaptive.AdaptiveReducer.active() */
  override def active: Receive =
    this.handleTaskEvent orElse this.handleTaskInitialized orElse super.active

  // methods to handle listeners
  private def addListener(listener: ActorRef) = {
    if (!(this.listeners contains listener)) {
      this.listeners = listener :: this.listeners
    }
  }
  private def removeListener(listener: ActorRef) = {
    this.listeners = this.listeners diff List(listener)
  }

  // following methods fire TaskListener trait methods such that they can react to event
  private def fireTaskRemoved(event: TaskEvent) {
    this.listeners foreach (_ ! TaskListener.TaskRemoved(event))
  }

  private def fireTaskAdded(event: TaskEvent) {
    this.listeners foreach (_ ! TaskListener.TaskAdded(event))
  }

  private def fireTaskPerformed(event: TaskEvent) {
    this.listeners foreach (_ ! TaskListener.TaskPerformed(event))
  }

  private def fireTaskInitialized(event: TaskEvent) {
    this.listeners foreach (_ ! TaskListener.TaskInitialized(event))
  }

  private def fireTaskDone(event: TaskEvent) {
    this.listeners foreach (_ ! TaskListener.TaskDone(event))
  }
}
