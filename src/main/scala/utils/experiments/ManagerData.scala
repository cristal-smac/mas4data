package utils.experiments

/** Represent the data of a manager during a run.
  *
  * @param contribution     contribution of the reducer during the run
  * @param avgEffectiveK    average effective k if the manager uses a k-eligible
  *                         strategy
  * @param taskSplitCount   number of task split done during the run
  * @param pauseSwitchCount number of pause state switch done during the run
  */
class ManagerData(
  val contribution: Long,
  val avgEffectiveK: Option[Double],
  val taskSplitCount: Int,
  val pauseSwitchCount: Int,
  val initialTasksByMaxOwnershipRate: Map[Double, Int],
  val initialTasksByOwnerOwnershipRate: Map[Double, Int]
) extends Data with Serializable {

  val fields: Map[String, String] = Map(
    "contribution"       -> this.contribution.toString,
    "avg. K"             -> this.avgEffectiveK.toString,
    "task split count"   -> this.taskSplitCount.toString,
    "pause switch count" -> this.pauseSwitchCount.toString
  )

}
