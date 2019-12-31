package utils.experiments

/** Represent the data of a monitor during a run.
  *
  * @param reducersData data from the reducers
  * @param reducingTime reducing time
  * @param timeFairness time fairness of the run
  */
class MonitorData(
  val reducersData: List[ReducerData],
  val reducingTime: Double,
  val timeFairness: Double
) extends Data {

  // Split contributions and brokers data
  private val (managerData, brokerData): (List[ManagerData], List[BrokerData]) =
    this.reducersData.foldLeft((List[ManagerData](), List[BrokerData]())) {
      case ((md, bd), rd) =>
        (rd.managerData :: md, rd.brokerData :: bd)
    }

  // Aggregate broker data fields
  private def aggregateFields(f: BrokerData => Int): Int =
    this.brokerData.foldLeft(0) {
      case (acc, bd) => acc + f(bd)
    }

  /** Total number of initialized CFP during the run. */
  val initCFP: Int = this aggregateFields { _.initCFP }

  /** Total number of canceled CFP during the run. */
  val canceledCFP: Int = this aggregateFields { _.canceledCFP }

  /** Total number of denied CFP during a run. */
  val deniedCFP: Int = this aggregateFields { _.deniedCFP }

  /** Total number of declined by all CFP during a run. */
  val declinedByAllCFP: Int = this aggregateFields { _.declinedByAllCFP }

  /** Total number of successful CFP during the run. */
  val successfulCFP: Int = this aggregateFields { _.successfulCFP }

  /** Total number of Propose during the run. */
  val receivedPropose: Int = this aggregateFields { _.receivedPropose }

  /** Fairness ratio of the run. */
  val ratio: Double = {
    val contributions = this.managerData map { _.contribution }

    contributions.min.toDouble / contributions.max.toDouble
  }

  /** Number of task split during the run. */
  val taskSplit: Int = (this.managerData map { _.taskSplitCount }).sum

  /** Chunks performed (total, locally). */
  val (performedChunks, locallyPerformedChunks): (Int, Int) =
    this.reducersData.foldLeft((0, 0)) {
      case ((perf, local), data) => (
        perf + data.workerData.totalPerformedChunks,
        local + data.workerData.totalLocallyPerformedChunks
      )
    }

  /** Average effective k if a k-eligible strategy was used during the run. */
  val avgEffectiveK: Option[Double] = {
    val sum = (this.managerData map { md => md.avgEffectiveK match {
      case Some(k) => k
      case None    => 0
    }}).sum
    val res = sum / this.managerData.length

    if (res == 0) None else Some(res)
  }

  /** Total waiting time during the run. */
  val totalWaitingTime: Long = this.reducersData.foldLeft(0.toLong) {
    case (acc, rd) => acc + rd.workerData.totalWaitingTime
  }

  /** Total working time during the run. */
  val totalWorkingTime: Double = this.reducersData.foldLeft(0.toDouble) {
    case (acc, rd) => acc + rd.workerData.totalWorkingTime
  }

  /** Working time fairness of the run. */
  val workingTimeFairness: Double = {
    val workingTimes = this.reducersData map { _.workerData.totalWorkingTime }

    workingTimes.min / workingTimes.max
  }

  val fields: Map[String, String] = Map(
    "init CFP"                  -> this.initCFP.toString,
    "canceled CFP"              -> this.canceledCFP.toString,
    "total denied CFP"          -> this.deniedCFP.toString,
    "total declined by all CFP" -> this.declinedByAllCFP.toString,
    "successful CFP"            -> this.successfulCFP.toString,
    "avg K"                     -> this.avgEffectiveK.toString,
    "task split"                -> this.taskSplit.toString,
    "performed chunks"          -> this.performedChunks.toString,
    "locally performed chunks"  -> this.locallyPerformedChunks.toString,
    "reducing time"             -> this.reducingTime.toString,
    "contrib. fairness"         -> this.ratio.toString,
    "time fairness"             -> this.timeFairness.toString,
    "working time fairness"     -> this.workingTimeFairness.toString,
    "total waiting time"        -> this.totalWaitingTime.toString,
    "total working time"        -> this.totalWorkingTime.toString,
    "total received propose"    -> this.receivedPropose.toString
  )

}
