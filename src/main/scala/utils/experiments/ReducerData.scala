package utils.experiments

/** Represent the data of a reducer during a run.
  *
  * @constructor create a new ReducerData instance
  * @param id id of the reducer
  * @param managerData data of the reducer's manager
  * @param brokerData data of the reducer's broker
  */
@SerialVersionUID(1L)
class ReducerData(
  val id: String,
  val managerData: ManagerData,
  val brokerData: BrokerData,
  val workerData: WorkerData,
  val foremanData: Map[Double, Int]
) extends Data with Serializable {

  val fields: Map[String, String] =
    Map("id" -> this.id) ++
    this.managerData.fields ++
    this.brokerData.fields ++
    this.workerData.fields

}
