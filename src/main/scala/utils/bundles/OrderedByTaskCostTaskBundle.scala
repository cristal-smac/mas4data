package utils.bundles

import akka.actor.ActorRef
import utils.tasks.Task

import scala.collection.mutable

/** Abstract class which represents task bundles which are ordered by the task
  * costs.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
abstract class OrderedByTaskCostTaskBundle(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef]
) extends TaskBundle(initialTasks, owner, rfhMap) {

  protected val ordering: Ordering[Task] =
    Ordering.comparatorToOrdering(
      new IncreasingCostTaskComparator(this.getTaskCostForOwner)
    )

  // Inner task collection
  protected val tasks: mutable.TreeSet[Task] =
    mutable.TreeSet.empty(this.ordering)

  /** Initiate the task bundle. */
  this.initialTasks foreach this.addTask

  /** Return true iff the task bundle is empty.
    *
    * @return true iff the task bundle is empty
    */
  override def isEmpty: Boolean = this.tasks.isEmpty

  /** Return the size of the task bundle (i.e. the number of tasks in the
    * bundle).
    *
    * @return the size of the task bundle
    */
  override def size: Int = this.tasks.size

  /** Return true iff the task bundle contains the given task.
    *
    * @param task task to know if it is in the bundle or not
    * @return true iff the task bundle contains the given task
    */
  override def containsTask(task: Task): Boolean = this.tasks.contains(task)

  /** Inner process to add a task to the bundle.
    *
    * @param task task to add in the bundle
    */
  override def addTaskToBundle(task: Task): Unit = this.tasks += task

  /** Inner process to remove a task from the bundle.
    *
    * @param task task to remove from the bundle
    */
  override def removeTaskFromBundle(task: Task): Unit = this.tasks -= task

  /** Show the biggest task of the bundle.
    *
    * @return the biggest task of the bundle
    */
  override def showBiggestTask: Task = this.tasks.last

}
