package utils.experiments

/** Represents the worker data of a run.
  *
  * @param totalPerformedChunks        number of chunks performed by this worker
  * @param totalLocallyPerformedChunks number of chunks locally performed by
  *                                    this worker
  * @param totalWorkingTime            total working time of the worker during
  *                                    the run
  * @param totalWaitingTime            total waiting time of the worker during
  *                                    the run
  * @param totalFreeTime               total time in which the worker is in free
  *                                    state
  * @param totalBusyTime               total time in which the worker is in busy
  *                                    state
  */
class WorkerData(
  val totalPerformedChunks: Int,
  val totalLocallyPerformedChunks: Int,
  val totalWorkingTime: Double,
  val totalWaitingTime: Long,
  val totalFreeTime: Long,
  val totalBusyTime: Long
) extends Data with Serializable {

  val fields: Map[String, String] = Map(
    "Performed chunks"         -> this.totalPerformedChunks.toString,
    "Locally performed chunks" -> this.totalLocallyPerformedChunks.toString,
    "Total waiting time (ms)"  -> this.totalWaitingTime.toString,
    "Total working time (ms)"  -> this.totalWorkingTime.toString,
    "Total free time (ms)"     -> this.totalFreeTime.toString,
    "Total busy time (ms)"     -> this.totalBusyTime.toString
  )

}
