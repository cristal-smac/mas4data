package utils.strategies.partition

import akka.actor.ActorRef
import utils.jobs.ReduceJob
import utils.tasks.{Task, TaskBuilder}

/** Partition strategy abstraction.
  *
  * A partition strategy distributes tasks in a given number of different
  * groups.
  */
abstract class PartitionStrategy {

  /** Distributes tasks in different task groups.
    *
    * @param tasks           tasks to partition
    * @param receivers       receivers of the tasks
    * @param currentWorkload current workload
    * @param rfhMap          map which associates receivers and their RFH
    * @return task partition
    */
  def partition(
    tasks: List[Task],
    receivers: List[ActorRef],
    currentWorkload: List[Long],
    rfhMap: Map[ActorRef, ActorRef]
  ): Map[ActorRef, List[Task]]

}

/** LPT partition strategy.
  *
  * Partition strategy which uses the LPT algorithm.
  */
case object LPTPartitionStrategy extends PartitionStrategy {

  /** Distributes tasks in different task groups.
    *
    * @param tasks           tasks to partition
    * @param receivers       receivers of the tasks
    * @param currentWorkload current workload
    * @param rfhMap          map which associates receivers and their RFH
    * @return task partition
    */
  override def partition(
    tasks: List[Task],
    receivers: List[ActorRef],
    currentWorkload: List[Long],
    rfhMap: Map[ActorRef, ActorRef]
  ): Map[ActorRef, List[Task]] = {
    val nbGroups = receivers.length
    val sortedTasks = tasks sortWith {
      case (t1, t2) => t1.nbValues > t2.nbValues
    }
    val initialResult = Vector.fill(nbGroups)(List[Task]())
    val neutralElement = (currentWorkload.zipWithIndex map (_.swap)).toMap

    def updateResult(
      result: Vector[List[Task]],
      listToFillIndex: Int,
      elementToAdd: Task
    ): Vector[List[Task]] =
      result.updated(listToFillIndex, elementToAdd :: result(listToFillIndex))

    def getLessLoadedGroupIndex(actualPartition: Map[Int, Long]): Int =
      (actualPartition minBy (_._2))._1

    val (finalPartition, _) =
      sortedTasks.foldLeft((initialResult, neutralElement)) {
        case ((result, actualPartition), task) =>
          val lessLoadedGroupIndex = getLessLoadedGroupIndex(actualPartition)
          val updatedResult = updateResult(result, lessLoadedGroupIndex, task)
          val previousGroupWorkload = actualPartition(lessLoadedGroupIndex)
          val newGroupWorkload = previousGroupWorkload + task.nbValues

          (
            updatedResult,
            actualPartition + (lessLoadedGroupIndex -> newGroupWorkload)
          )
      }

    (receivers zip finalPartition).toMap
  }

}

/** Naive partition strategy (using hash code on the key).
  *
  * @param reduceJob reduceJob to use to recover the map key
  */
case class NaivePartitionStrategy(
  reduceJob: ReduceJob
) extends PartitionStrategy {

  /** Distributes tasks in different task groups.
    *
    * @param tasks           tasks to partition
    * @param receivers       receivers of the tasks
    * @param currentWorkload current workload
    * @param rfhMap          map which associates receivers and their RFH
    * @return task partition
    */
  override def partition(
    tasks: List[Task],
    receivers: List[ActorRef],
    currentWorkload: List[Long],
    rfhMap: Map[ActorRef, ActorRef]
  ): Map[ActorRef, List[Task]] = {
    val nbGroups = receivers.length
    val groupedKeyByModulo = tasks groupBy {
      task =>
        val key = this.reduceJob.recoverMapKey(task.key)

        key.hashCode.abs % nbGroups
    }
    val tasksByPerformer = (0 until nbGroups) map {
      x => groupedKeyByModulo.getOrElse(x, Nil)
    }

    (receivers zip tasksByPerformer).toMap
  }

}

/** Partition strategy which give a task to the agent which have the higher
  * ownership rate.
  *
  * @param reduceJob reduce job to use to recover the map key
  */
case class OwnershipPartitionStrategy(
  reduceJob: ReduceJob
) extends PartitionStrategy {

  // Compute ownership rate for a task and an agent
  private def getOwnershipRateFor(
    task: Task,
    agent: ActorRef,
    rfhMap: Map[ActorRef, ActorRef]
  ): Double = task.ownershipRate(rfhMap(agent))

  // Compute task cost for a task and an agent
  private def getTaskCostFor(
    task: Task,
    agent: ActorRef,
    rfhMap: Map[ActorRef, ActorRef]
  ): Long = task.cost(rfhMap(agent))

  /** Distributes tasks in different task groups.
    *
    * @param tasks           tasks to partition
    * @param receivers       receivers of the tasks
    * @param currentWorkload current workload
    * @param rfhMap          map which associates receivers and their RFH
    * @return task partition
    */
  override def partition(
    tasks: List[Task],
    receivers: List[ActorRef],
    currentWorkload: List[Long],
    rfhMap: Map[ActorRef, ActorRef]
  ): Map[ActorRef, List[Task]] = {
    // Neutral element of the following foldLeft operation
    val neutralElem =
      Map[ActorRef, (List[Task], Long)]() withDefaultValue (List[Task](), 0.toLong)
    // Assign tasks to the agent which has the maximum ownership rate
    val (intermedResults, nonAssignatedTasks) =
      tasks.foldLeft((neutralElem, List[Task]())) {
        case ((acc, nonAssignated), task) =>
          val potentialReceivers = receivers.filter(
            this.getOwnershipRateFor(task, _, rfhMap) == task.maximumOwnershipRate
          )

          if (potentialReceivers.isEmpty) {
            (acc, task :: nonAssignated)
          } else {
            val receiver = potentialReceivers.minBy(acc(_)._2)

            if (acc.contains(receiver)) {
              val (previousBundle, previousWorklaod) = acc(receiver)

              (
                acc.updated(
                  receiver,
                  (
                    task :: previousBundle,
                    previousWorklaod + this.getTaskCostFor(task, receiver, rfhMap)
                  )
                ),
                nonAssignated
              )
            } else {
              (
                acc + (receiver -> (List(task), this.getTaskCostFor(task, receiver, rfhMap))),
                nonAssignated
              )
            }
          }
      }
    // Add agents which have not been used to the results
    val intermedWithEmptyReceiver: Map[ActorRef, (List[Task], Long)] =
      intermedResults ++
      receivers.filterNot(intermedResults.contains).map(_ -> (Nil, 0.toLong))

    // Assign tasks which have not been given to the less loaded agents
    nonAssignatedTasks.foldLeft(intermedWithEmptyReceiver) {
      case (acc, task) =>
        val receiver = acc.keys.minBy(acc(_)._2)
        val (previousBundle, previousWorkload) = acc(receiver)

        acc.updated(
          receiver,
          (
            task :: previousBundle,
            previousWorkload + this.getTaskCostFor(task, receiver, rfhMap)
          )
        )
    } map { case (receiver, (receiverTasks, _)) => receiver -> receiverTasks }
  }

}

case object GeneratedDedicatedPartitionStrategy extends PartitionStrategy {

  private def throwNonLocalChunk(
    key: String,
    mapperNb: Int,
    reducerNb: Int
  ): Boolean = //mapperNb != reducerNb && key.hashCode % 17 == 2
    false

  /** Distributes tasks in different task groups.
    *
    * @param tasks           tasks to partition
    * @param receivers       receivers of the tasks
    * @param currentWorkload current workload
    * @param rfhMap          map which associates receivers and their RFH
    * @return task partition
    */
  override def partition(
    tasks: List[Task],
    receivers: List[ActorRef],
    currentWorkload: List[Long],
    rfhMap: Map[ActorRef, ActorRef]
  ): Map[ActorRef, List[Task]] =
    tasks.foldLeft(Map[ActorRef, List[Task]]()) {
      case (acc, task) =>
        val splitKey = task.key.split("_")
        val key = splitKey(0)
        val reducerNumber = splitKey(1).toInt
        // val mapperNumber = splitKey(2).toInt
        // val reducer = receivers(reducerNumber % receivers.length)
        val reducer = receivers(reducerNumber % receivers.length)
        val newTask = TaskBuilder.buildTask(key, task.chunks)

        if (acc contains reducer) {
          acc.updated(reducer, newTask :: acc(reducer))
        } else {
          acc + (reducer -> List(newTask))
        }
    }

}
