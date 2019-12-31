package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Props}
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.debugs.Debug
import utils.strategies.partition.{GeneratedDedicatedPartitionStrategy, PartitionStrategy}
import utils.tasks.{Task, TaskBuilder}

/** Companion object of the partitioner agent.
  *
  * Contain all the messages that a partitioner is able to handle.
  */
object Partitioner {

  /** Message Partition.
    *
    * Ask the partitioner to partition tasks.
    *
    * @param tasks  tasks to partition
    * @param index  index of the sent task bundle
    * @param isLast inform the partition if the bundle is the last to receive
    *               from the mapper which initiates the message
    */
  case class Partition(tasks: List[Task], index: Int, isLast: Boolean)

  /** Message SelfPartition.
    *
    * Self message to launch the partitioning
    *
    * @param tasks tasks to partition
    */
  case class SelfPartition(tasks: List[Task])

  /** Message WorkOnOk
    *
    * Acknowledgment of the AdaptiveReducer.WorkOn message.
    *
    * @param index index of the task bundle which is well received
    */
  case class WorkOnOk(index: Int)

}

/** Partitioner agent.
  *
  * The partitioner makes the task partition for the reducers.
  *
  * @param partitionStrategy partition strategy used by the partitioner
  * @param senders           agents which will send tasks to partition
  * @param taskPerformers    agents to partition tasks for
  */
class Partitioner(
  partitionStrategy: PartitionStrategy,
  senders: Set[ActorRef],
  taskPerformers: List[ActorRef]
) extends Actor with Debug {

  this.setDebug(false)

  // Configuration object
  private val config = ConfigurationBuilder.config

  // Map which associates its agents and their RFH
  private var rfhMap: Map[ActorRef, ActorRef] = _

  /** Associate task performers with their tasks from a previously computed
    * task partition.
    *
    * @param taskPartition task partition
    * @return map which associates a task performer with its tasks
    */
  protected def getPerformerTasksAssociationFromTaskPartition(
    taskPartition: Map[ActorRef, List[Task]]
  ): Map[ActorRef,  Iterator[List[Task]]] = {
    taskPartition map {
      case (key, tasks) => key -> tasks.grouped(1000)
    }
  }

  /** Group all the received tasks by key.
    *
    * @param tasks tasks to group by key
    * @return list of tasks with only one task by key
    */
  protected def groupTasksByKey(tasks: List[Task]): List[Task] = {
    val subTasksByKeys = tasks.groupBy(_.key)

    subTasksByKeys.foldRight(List[Task]()) {
      case ((_, subTasks), acc) =>
        val completeTask = subTasks.tail.foldLeft(subTasks.head) {
          case (inProgressTask, task) =>
            TaskBuilder.mergeTasks(inProgressTask, task)
        }

        completeTask :: acc
    }
  }

  // Send a task bundle to a performer and create associated new timer
  private def createNewTimerAndSendTaskBundle(
    performer: ActorRef,
    performersAndJustSent: Map[ActorRef, List[Task]],
    performersAndIndexes: Map[ActorRef, Int]
  ): ActorRef = {
    val newTimer = context actorOf Props(
      classOf[Timer], this.config.timeouts("acknowledgment-timeout")
    )
    val index = performersAndIndexes(performer)
    val toSend = performersAndJustSent(performer)

    debugSend("AdaptiveReducer.WorkOn", "waitReducers")
    performer ! AdaptiveReducer.WorkOn(toSend, index)
    debugSend("Timer.Start", "waitReducers")
    newTimer ! Timer.Start
    newTimer
  }

  /** Kill the partitioner agent. */
  protected def die(): Unit = {
    // Inform the monitor that the task partition is done
    debugSend("Monitor.PartitionDone", "dying")
    context.parent ! Monitor.PartitionDone
    context stop self
  }

  // Send tasks to performers and go to waitReducersState
  private def goToWaitReducerState(
    performersToRelaunch: List[ActorRef],
    performersAndJustSent: Map[ActorRef, List[Task]],
    performersAndTasks: Map[ActorRef, Iterator[List[Task]]],
    performersAndIndexes: Map[ActorRef, Int],
    performersAndTimers: Map[ActorRef, ActorRef],
    performersToProvide: Set[ActorRef]
  ): Unit = {
    // Send bundle to each performer to relaunch and use a new timer for each
    // one of them
    val newPerformersAndTimers =
      performersToRelaunch.foldLeft(performersAndTimers) {
        case (acc, performer) =>
          val newTimer = this.createNewTimerAndSendTaskBundle(
            performer,
            performersAndJustSent,
            performersAndIndexes
          )

          acc.updated(performer, newTimer)
      }

    context become (
      this.waitReducers(
        performersAndJustSent,
        performersAndTasks,
        performersAndIndexes,
        newPerformersAndTimers,
        performersToProvide
      )
      orElse this.handleDeprecatedPartition("waitReducers")
      orElse this.handleDeprecatedWorkOnOk(performersAndIndexes)
      orElse this.handleDeprecatedTimeout
      orElse this.handleUnexpected("waitReducers")
    )
  }

  override def receive: Receive = {
    val ones = for (_ <- 1 to this.senders.size) yield 1
    val initialMap = (this.senders zip ones).toMap

    this.receiveTasks(this.senders, initialMap, Nil) orElse
    this.handleDeprecatedPartitionWhileReceiving(initialMap) orElse
    this.handleUnexpected("receiveTasks")
  }

  /** State in which the partitioner receives tasks to partition.
    *
    * @param senders           agents which send tasks to partition
    * @param sendersAndIndexes senders and indexes of the received task bundle
    * @param receivedTasks     task already received
    */
  def receiveTasks(
    senders: Set[ActorRef],
    sendersAndIndexes: Map[ActorRef, Int],
    receivedTasks: List[Task]
  ): Receive = {

    case AdaptiveReducer.BroadcastRFH(map) =>
      debugReceive("BroadcastRFH", sender, "receiveTasks")
      this.rfhMap = map
      debugSend("Monitor.BroadcastRFHOk","receiveTasks")
      sender ! Monitor.BroadcastRFHOk

    case Partitioner.Partition(tasks, index, isLast)
      if senders.contains(sender) &&
         sendersAndIndexes(sender) == index &&
         isLast =>
      debugReceive("Partitioner.Partition", sender, "receiveTasks")

      val newSenders = senders - sender
      val newTasks = receivedTasks ++ tasks

      debugSend("AdaptiveMapper.PartitionOk(" + index + ")", "receiveTasks")
      sender ! AdaptiveMapper.PartitionOk(index)

      if (newSenders.isEmpty) {
        debugSend("Monitor.StartPartitioning", "receiveTasks")
        context.parent ! Monitor.StartPartitioning
        debugSend("Partitioner.Partition to itself", "receiveTasks")
        self ! Partitioner.SelfPartition(newTasks)
        context become (
          this.partition
          orElse this.handleDeprecatedPartition("partition")
          orElse this.handleUnexpected("partition")
        )
      } else {
        context become (
          this.receiveTasks(newSenders, sendersAndIndexes, newTasks)
          orElse this.handleDeprecatedPartitionWhileReceiving(
            sendersAndIndexes
          ) orElse this.handleUnexpected("receiveTasks")
        )
      }

    case Partitioner.Partition(tasks, index, _)
      if senders.contains(sender) && sendersAndIndexes(sender) == index =>
      debugReceive("Partitioner.Partition", sender, "receiveTasks")
      debugSend("AdaptiveMapper.PartitionOk(" + index + ")", "receiveTasks")
      sender ! AdaptiveMapper.PartitionOk(index)

      val newSendersAndIndexes =
        sendersAndIndexes.updated(sender, sendersAndIndexes(sender) + 1)

      context become (
        this.receiveTasks(
          senders,
          newSendersAndIndexes,
          receivedTasks ++ tasks
        ) orElse this.handleDeprecatedPartitionWhileReceiving(
          newSendersAndIndexes
        ) orElse this.handleUnexpected("receiveTasks")
      )

  }

  /** State in which the partitioner do the actual task partition. */
  def partition: Receive = {

    case Partitioner.SelfPartition(tasks) if sender == self =>
      debugReceive("Partitioner.Partition", sender, "partition")

      val nbPerformers = this.taskPerformers.length
      val tasksGroupedByKey = this.groupTasksByKey(tasks)
      val taskPartition = this.partitionStrategy.partition(
        tasksGroupedByKey,
        this.taskPerformers,
        List.fill(taskPerformers.length)(0),
        this.rfhMap
      )
      val performersAndTasks =
        this.getPerformerTasksAssociationFromTaskPartition(taskPartition)
      val ones = for (_ <- 1 to nbPerformers) yield 1
      val performersAndIndexes = (this.taskPerformers zip ones).toMap
      val performersAndToSend = performersAndTasks map {
        case (performer, performerTasks) =>
          if (performerTasks.hasNext)
            performer -> performerTasks.next
          else
            performer -> Nil
      }

      this.goToWaitReducerState(
        this.taskPerformers,
        performersAndToSend,
        performersAndTasks,
        performersAndIndexes,
        Map[ActorRef, ActorRef](),
        this.taskPerformers.toSet
      )

  }

  /** State in which the partitioner waits for reducers acknowledgment.
    *
    * @param performersAndJustSent association between performers and the task
    *                              bundle which has just been sent to it
    * @param performersAndTasks   association between performers and tasks
    * @param performersAndIndexes association between performers and the index
    *                             of the task bundle currently sent
    * @param performersAndTimers  association between performers and their
    *                             dedicated timer
    * @param performersToProvide  performers which still have tasks to receive
    */
  def waitReducers(
    performersAndJustSent: Map[ActorRef, List[Task]],
    performersAndTasks: Map[ActorRef, Iterator[List[Task]]],
    performersAndIndexes: Map[ActorRef, Int],
    performersAndTimers: Map[ActorRef, ActorRef],
    performersToProvide: Set[ActorRef]
  ): Receive = {

    // The performer has received all its task bundles
    case Partitioner.WorkOnOk(index)
      if performersToProvide.contains(sender) &&
      performersAndIndexes(sender) == index &&
      !performersAndTasks(sender).hasNext =>
      debugReceive(
        "Partitioner.WorkOnOk(" + index + ")",
        sender,
        "waitReducers"
      )
      // Cancel the associated timer
      debugSend("Timer.Cancel", "waitReducers")
      performersAndTimers(sender) ! Timer.Cancel

      // The performer is no longer to provide
      val newPerformersToProvide = performersToProvide - sender

      // It remains performers to provide
      if (newPerformersToProvide.nonEmpty) {
        context become (
          this.waitReducers(
            performersAndJustSent,
            performersAndTasks,
            performersAndIndexes,
            performersAndTimers,
            newPerformersToProvide
          )
          orElse this.handleDeprecatedPartition("waitReducers")
          orElse this.handleDeprecatedWorkOnOk(performersAndIndexes)
          orElse this.handleDeprecatedTimeout
          orElse this.handleUnexpected("waitReducers")
        )
      }
      // All the performers have received all their tasks
      else {
        this.die()
      }

    // A performer is ready to receive another task bundle
    case Partitioner.WorkOnOk(index)
      if performersToProvide.contains(sender) &&
      performersAndIndexes(sender) == index =>
      debugReceive(
        "Partitioner.WorkOnOk(" + index + ")",
        sender,
        "waitReducers"
      )
      // Cancel the associated timer
      debugSend("Timer.Cancel", "waitReducers")
      performersAndTimers(sender) ! Timer.Cancel

      // Update the index of the performer
      val newPerformersAndIndexes = performersAndIndexes.updated(
        sender,
        performersAndIndexes(sender) + 1
      )
      // Update the bundle which is sent to the performer
      val newPerformersAndJustSent = performersAndJustSent.updated(
        sender,
        performersAndTasks(sender).next
      )
      // Create a new timer and send a new task bundle
      val newTimer = this.createNewTimerAndSendTaskBundle(
        sender,
        newPerformersAndJustSent,
        newPerformersAndIndexes
      )

      context become (
        this.waitReducers(
          newPerformersAndJustSent,
          performersAndTasks,
          newPerformersAndIndexes,
          performersAndTimers.updated(sender, newTimer),
          performersToProvide
        )
        orElse this.handleDeprecatedPartition("waitReducers")
        orElse this.handleDeprecatedWorkOnOk(newPerformersAndIndexes)
        orElse this.handleDeprecatedTimeout
        orElse this.handleUnexpected("waitReducers")
      )

    // A performer has not answered before the timeout
    case Timer.Timeout if performersAndTimers.values.toList contains sender =>
      debugReceive("Timer.Timeout", sender, "waitReducers")

      // Reverse of the performersAndTimers map
      // (i.e. timer -> performer instead of performer -> timer)
      val timersAndPerformers = performersAndTimers.map(_.swap)

      this.goToWaitReducerState(
        List(timersAndPerformers(sender)),
        performersAndJustSent,
        performersAndTasks,
        performersAndIndexes,
        performersAndTimers,
        performersToProvide
      )

  }

  /** Handle deprecated Partition messages while the partitioner receives tasks
    * to partition.
    *
    * @param sendersAndIndexes map which associates the senders and the current
    *                          index of the exchanges
    */
  private def handleDeprecatedPartitionWhileReceiving(
    sendersAndIndexes: Map[ActorRef, Int]
  ): Receive = {

    case Partitioner.Partition(_, index, _)
      if this.senders.contains(sender) && sendersAndIndexes(sender) < index =>
      debugDeprecated("Partitioner.Partition", sender, "receiveTasks")
      debugSend("PartitionOk(" + index + ")", "receiveTasks")
      sender ! AdaptiveMapper.PartitionOk(index)

  }

  /** Handle deprecated Partition messages.
    *
    * @param state state in which the deprecated Partition message is received
    */
  private def handleDeprecatedPartition(state: String): Receive = {

    case Partitioner.Partition(_, index, _) if this.senders.contains(sender) =>
      debugDeprecated("Partitioner.Partition", sender, state)
      debugSend("PartitionOk(" + index + ")", state)
      sender ! AdaptiveMapper.PartitionOk(index)

  }

  /** Handle deprecated WorkOnOk messages.
    *
    * @param performersAndIndexes association between performers and the index
    *                             of the task bundle currently sent
    */
  private def handleDeprecatedWorkOnOk(
    performersAndIndexes: Map[ActorRef, Int]
  ): Receive = {

    case Partitioner.WorkOnOk(index)
      if this.taskPerformers.contains(sender) &&
      index < performersAndIndexes(sender) =>
      debugDeprecated(
        "Partition.WorkOnOk(" + index + ")",
        sender,
        "waitReducers"
      )

  }

  /** Handle deprecated Timeout messages. */
  private def handleDeprecatedTimeout: Receive = {

    case Timer.Timeout =>
      debugDeprecated("Timer.Timeout", sender, "waitReducers")

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  private def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}

