package utils.config

import akka.actor.ActorRef
import utils.bundles._
import utils.tasks.Task

object TaskBundleHandler {

  // Pattern to identify the k-eligible-small strategy
  private val kEligibleSmallPattern = """\(k-eligible-small, (\d+)\)""".r

  // Pattern to identify the k-eligible-big strategy
  private val kEligibleBigPattern = """\(k-eligible-big, (\d+)\)""".r

  /** Return the task bundle corresponding to the strategy option.
    *
    * @param strategy strategy option in the configuration file
    * @return task bundle corresponding to the strategy
    */
  def getTaskBundleFrom(
    strategy: String,
    initialTasks: List[Task],
    owner: ActorRef,
    rfhMap: Map[ActorRef, ActorRef],
    threshold: Double
  ): TaskBundle = strategy match {
      case "cbds" =>
        if (threshold < 1)
          new CBDS(initialTasks, owner, rfhMap)
        else
          new CBDS(
            initialTasks,
            owner,
            rfhMap
          ) with NoDelegationBundle
      case "csdb" =>
        if (threshold < 1)
          new CSDB(initialTasks, owner, rfhMap)
        else
          new CSDB(
            initialTasks,
            owner,
            rfhMap
          ) with NoDelegationBundle

      case kEligibleSmallPattern(kMax) =>
        if (threshold < 1)
          new KEligibleTaskBundleSmallTaskPerformedFirst(
            initialTasks,
            owner,
            rfhMap,
            kMax.toInt
          )
        else
          new KEligibleTaskBundleSmallTaskPerformedFirst(
            initialTasks,
            owner,
            rfhMap,
            kMax.toInt
          ) with NoDelegationBundle

      case kEligibleBigPattern(kMax) =>
        if (threshold < 1)
          new KEligibleTaskBundleBigTaskPerformedFirst(
            initialTasks,
            owner,
            rfhMap,
            kMax.toInt
          )
        else
          new KEligibleTaskBundleBigTaskPerformedFirst(
            initialTasks,
            owner,
            rfhMap,
            kMax.toInt
          ) with NoDelegationBundle

      case "ownership" =>
        if (threshold < 1)
          new OwnershipRateTaskBundle(
            initialTasks,
            owner,
            rfhMap
          )
        else
          new OwnershipRateTaskBundle(
            initialTasks,
            owner,
            rfhMap
          ) with NoDelegationBundle
    }

}
