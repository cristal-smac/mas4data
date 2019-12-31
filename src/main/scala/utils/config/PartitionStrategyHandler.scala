package utils.config

import utils.jobs.ReduceJob
import utils.strategies.partition._

/** Handle the partition strategy of a run. */
object PartitionStrategyHandler {

  /** Build a partition strategy from a string description and a reduce job.
    *
    * @param strategy  string description of the partition strategy
    * @param reduceJob reduce job used during the run
    * @return partition strategy
    */
  def buildPartitionStrategy(
    strategy: String,
    reduceJob: ReduceJob
  ): PartitionStrategy = strategy match {
    case "lpt"       => LPTPartitionStrategy
    case "naive"     => NaivePartitionStrategy(reduceJob)
    case "ownership" => OwnershipPartitionStrategy(reduceJob)
    case "generated" => GeneratedDedicatedPartitionStrategy
  }

}
