package utils.bundles

import akka.actor.ActorRef
import utils.tasks.Task

import scala.collection.mutable

/** Task bundle.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
abstract class TaskBundle(
  initialTasks: List[Task],
  val owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef]
) {

  /** Current workload of the owner. */
  protected var currentOwnerWorkload: Long = 0

  /** Return true iff the task bundle is empty.
    *
    * @return true iff the task bundle is empty
    */
  def isEmpty: Boolean

  /** Return true iff the task bundle is not empty.
    *
    * @return true iff the task bundle is not empty
    */
  def nonEmpty: Boolean = !this.isEmpty

  /** Return the owner workload.
    *
    * @return current owner workload
    */
  def ownerWorkload: Long = this.currentOwnerWorkload

  /** Return the size of the task bundle (i.e. the number of tasks in the
    * bundle).
    *
    * @return the size of the task bundle
    */
  def size: Int

  /** Return true iff the task bundle contains the given task.
    *
    * @param task task to know if it is in the bundle or not
    * @return true iff the task bundle contains the given task
    */
  def containsTask(task: Task): Boolean

  /** Compute the cost of a task for a given agent.
    *
    * @param task  task to know the cost of
    * @param agent agent to compute the task cost for
    * @return the given task cost for the given agent
    */
  protected def getTaskCostFor(task: Task, agent: ActorRef): Long =
    task.cost(this.rfhMap(agent))

  /** Compute the ownership rate of a task for a given agent.
    *
    * @param task task to know the ownership rate of
    * @param agent agent to compute the ownership rate for
    * @return the given task ownership rate for the given agent
    */
  protected def getTaskOwnershipRateFor(task: Task, agent: ActorRef): Double =
    task.ownershipRate(this.rfhMap(agent))

  /** Compute the cost of a task for the owner of the task bundle.
    *
    * @param task task to compute the cost
    * @return the given task cost for the task bundle owner
    */
  protected def getTaskCostForOwner(task: Task): Long =
    this.getTaskCostFor(task, this.owner)

  /** Compute the ownership rate of a task for the owner of the task bundle.
    *
    * @param task task to compute the ownership rate
    * @return the given task ownership rate for the task bundle owner
    */
  protected def getTaskOwnershipRateForOwner(task: Task): Double =
    this.getTaskOwnershipRateFor(task, this.owner)

  /** Inner process to add a task to the task bundle.
    *
    * @param task task to add to the bundle
    */
  protected def addTaskToBundle(task: Task): Unit

  /** Add a task to the bundle.
    *
    * @param task task to add in the bundle
    */
  def addTask(task: Task): Unit = {
    this.currentOwnerWorkload += this.getTaskCostForOwner(task)
    this.addTaskToBundle(task)
  }

  /** Inner process to remove a task from the task bundle.
    *
    * @param task task to remove from the bundle
    */
  protected def removeTaskFromBundle(task: Task): Unit

  /** Remove a task from the bundle.
    *
    * @param task task to remove from the bundle
    */
  def removeTask(task: Task): Unit = {
    this.currentOwnerWorkload -= this.getTaskCostForOwner(task)
    this.removeTaskFromBundle(task)
  }

  /** Return the next task to delegate.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to delegate
    */
  def nextTaskToDelegate(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task]

  /** Return the next task to perform.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to perform
    */
  def nextTaskToPerform(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task]

  /** Return the owner workload.
    *
    * @return the owner workload
    */
  def getOwnerWorkload: Long = this.currentOwnerWorkload

  /** Show the biggest task of the bundle.
    *
    * @return the biggest task of the bundle
    */
  def showBiggestTask: Task

  /** Extract the biggest task of the bundle.
    *
    * @return the biggest task of the bundle
    */
  def extractBiggestTask: Task = {
    val biggestTask = this.showBiggestTask

    this.removeTask(biggestTask)
    biggestTask
  }

  /** Apply the task split process on the task bundle.
    *
    * @param ownerWorkload current workload of the owner
    * @param workloadMap   map which associates agents and their workload
    * @return the potential key of the split task
    */
  def applySplitProcess(
    ownerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[String] = None

  /** Determine if an agent can take a task from the owner in charge.
    *
    * @param ownerWorkload    owner current workload
    * @param agentWorkload    workload of the other agent
    * @param taskCostForAgent cost of the task for the agent
    * @param threshold        threshold
    * @return true iff the agent can take the task from the owner in charge
    */
  protected def canTakeTaskInCharge(
    ownerWorkload: Long,
    agentWorkload: Long,
    taskCostForAgent: Long,
    threshold: Double
  ): Boolean = {
    val x = ownerWorkload - (agentWorkload + taskCostForAgent)
    val benefit = x.toDouble / ownerWorkload

    benefit > threshold
  }

  val initialTasksByMaxOwnershipRate: Map[Double, Int] =
    this.initialTasks.groupBy(_.maximumOwnershipRate) map {
      case (ownershipRate, tasks) => ownershipRate -> tasks.length
    }

  val initialTasksByOwnerOwnershipRate: Map[Double, Int] =
    this.initialTasks.groupBy(this.getTaskOwnershipRateForOwner) map {
      case (ownershipRate, tasks) => ownershipRate -> tasks.length
    }

}
