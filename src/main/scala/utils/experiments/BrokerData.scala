package utils.experiments

/** Represents the data of a broker during a run.
  *
  * @param initCFP          number of initialized CFP during a run
  * @param canceledCFP      number of canceled CFP during a run
  * @param deniedCFP        number of denied CFP during a run
  * @param declinedByAllCFP number of declined by all CFP during a run
  * @param successfulCFP    number of successful CFP during a run
  * @param receivedPropose  number of Propose messages received during a run
  */
class BrokerData(
  val initCFP: Int,
  val canceledCFP: Int,
  val deniedCFP: Int,
  val declinedByAllCFP: Int,
  val successfulCFP: Int,
  val receivedPropose: Int,
  val averageNegotiatedTaskCosts: Double
) extends Data with Serializable {

  val fields: Map[String, String] = Map(
    "init. CFP"                     -> this.initCFP.toString,
    "canceled CFP"                  -> this.canceledCFP.toString,
    "denied CFP"                    -> this.deniedCFP.toString,
    "declined by all CFP"           -> this.declinedByAllCFP.toString,
    "successful CFP"                -> this.successfulCFP.toString,
    "received Propose"              -> this.receivedPropose.toString,
    "average negotiated task costs" -> this.averageNegotiatedTaskCosts.toString
  )

}
