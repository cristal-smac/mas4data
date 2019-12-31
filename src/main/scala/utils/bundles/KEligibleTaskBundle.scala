package utils.bundles

import akka.actor.ActorRef
import utils.config.ConfigurationBuilder
import utils.tasks.Task

import scala.collection.mutable

/** Abstract class which represents k-eligible task bundles. A k-eligible task
  * bundle offer tasks to delegate for which at least k agents are able to
  * make a proposal.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
abstract class KEligibleTaskBundle(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef],
  kMax: Int
) extends OrderedByTaskCostTaskBundle(initialTasks, owner, rfhMap) {

  private lazy val threshold: Double = ConfigurationBuilder.config.threshold

  // List of effective values of k
  private val effectiveKs: mutable.ListBuffer[Int] = new mutable.ListBuffer()

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
  ): Option[Task] = {
    // Map[K, List[(Task, Representative, Representative workload)]]
    val kEligibleTasks = this.tasks.foldLeft(
        Map[Int, List[(Task, ActorRef, Long)]]() withDefaultValue Nil
    ) {
      case (acc, task) =>
        // Get the workloads of the agents which could make a proposal for the
        // current task
        val acceptingAgentsAndWorkloads = workloadMap.collect {
          case a@(agent, agentsWorkload) if this.canTakeTaskInCharge(
            currentOwnerWorkload,
            agentsWorkload,
            this.getTaskCostFor(task, agent),
            this.threshold
          ) => a
        }

        // The task is at least 1-eligible and may be added to the accumulator
        if (acceptingAgentsAndWorkloads.nonEmpty) {
          // Compute all the values of k for which we still have to search
          // tasks
          val kSearch = if (acc.keys.nonEmpty) {
            this.kMax to acc.keys.max by -1
          } else {
            this.kMax to 1 by -1
          }

          // Update the accumulator by adding the task to the value of k for
          // which it is eligible
          kSearch.filter(_ <= acceptingAgentsAndWorkloads.size).foldLeft(acc) {
            case (buildingAcc, currentK) =>
              val (representative, representativeWorkload) =
                acceptingAgentsAndWorkloads.maxBy(_._2)
              val toAdd = (task, representative, representativeWorkload)

              buildingAcc.updated(
                currentK,
                // In the worst case, the most loaded agent would take the
                // tasks in charge
                toAdd :: buildingAcc(currentK)
              )
          }
        }
        // The task is not eligible and so is not added to the accumulator
        else {
          acc
        }
    }

    // If there is at least one k-eligible task
    if (kEligibleTasks.nonEmpty) {
      // Effective value of k
      val effectiveK =  kEligibleTasks.keys.max
      // The evaluated tasks are the ones which have the higher value of
      // k-eligibility
      val potentiallyDelegatedTasks = kEligibleTasks(effectiveK)
      // The selected task is the one which minimize the maximum contribution
      // after the task delegation
      val (selectedTask, _, _) = potentiallyDelegatedTasks.minBy {
        case (task, representative, representativeWorkload) =>
          val ownerWorkloadWithoutTask =
            currentOwnerWorkload - this.getTaskCostForOwner(task)
          val representativeWithTaskWorkload =
            representativeWorkload + this.getTaskCostFor(task, representative)

          ownerWorkloadWithoutTask max representativeWithTaskWorkload
      }

      this.effectiveKs += effectiveK
      Some(selectedTask)
    }
    else {
      None
    }
  }

  /** Return the average value of k during the all run.
    *
    * @return the average value of k during the run
    */
  def getAverageEffectiveK: Double =
    this.effectiveKs.sum.toDouble / this.effectiveKs.size

}

/** Abstract class which represents k-eligible task bundles. A k-eligible task
  * bundle offer tasks to delegate for which at least k agents are able to
  * make a proposal.
  *
  * This specific k-eligible task bundle offer the big tasks to perform.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
class KEligibleTaskBundleBigTaskPerformedFirst(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef],
  kMax: Int
) extends KEligibleTaskBundle(initialTasks, owner, rfhMap, kMax) {

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
  ): Option[Task] = this.tasks.lastOption

}

/** Abstract class which represents k-eligible task bundles. A k-eligible task
  * bundle offer tasks to delegate for which at least k agents are able to
  * make a proposal.
  *
  * This specific k-eligible task bundle offer the small tasks to perform.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
class KEligibleTaskBundleSmallTaskPerformedFirst(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef],
  kMax: Int
) extends KEligibleTaskBundle(initialTasks, owner, rfhMap, kMax) {

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
