package utils.bundles

import akka.actor.ActorRef
import utils.tasks.Task

import scala.collection.mutable

/** Naive task bundle.
  *
  * The task bundle management strategy CBDS consumes/performs the small tasks and delegates the big ones
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
class CSDB(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef]
) extends OrderedByTaskCostTaskBundle(initialTasks, owner, rfhMap) {

  /** Return the next task to delegate.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to delegate
    */
  override def nextTaskToDelegate(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task] = this.tasks.lastOption

  /** Return the next task to perform.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to perform
    */
  override def nextTaskToPerform(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task] = this.tasks.headOption

}
