package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Props, Stash}
import utils.bundles.TaskBundle
import utils.config.ConfigurationBuilder
import utils.experiments.{BrokerData, ManagerData, ReducerData, WorkerData}
import utils.jobs.ReduceJob
import utils.strategies.split.{TaskSplitAllowed, TaskSplitDenied, TaskSplitPermission}
import utils.tasks._

import scala.collection.mutable

/** Companion object of the AdaptiveReducer class.
  *
  * Contain all the messages that an adaptive reducer could receive.
  */
object AdaptiveReducer {

  // ----- MONITOR ----- //

  /** Message Kill
    *
    * Kill the reducer.
    */
  case object Kill

  /** Message GetRFH
    *
    * Ask the reducer about its RFH.
    */
  case object GetRFH

  /** Message BroadcastRFH.
    *
    * Inform the reducer of the reducers distribution among RFH.
    *
    * @param rfhMap map which associate a reducer to a RFH
    */
  case class BroadcastRFH(rfhMap: Map[ActorRef, ActorRef])

  /** Message Acquaintances
    *
    * Inform the reducer of its acquaintances network.
    *
    * @param acquaintances acquaintances network of the reducer
    */
  case class Acquaintances(acquaintances: Set[ActorRef])

  // ----- PARTITIONER ----- //

  /** Message WorkOn
    *
    * Ask to the reducer to work on given tasks.
    *
    * @param tasks   tasks to work on
    * @param index   index of the task bundle of the WorkOn message
    */
  case class WorkOn(tasks: List[Task], index: Int)

  /** Message Execute
    *
    * Ask to the reducer to execute its job on the tasks it received from the
    * mappers.
    */
  case object Execute

  // ----- OTHER REDUCERS ----- //

  /** Message Accept
    *
    * Rely of a Broker.Accept message.
    *
    * @param task task that represent the data
    * @param id   id of the CFP
    */
  case class Accept(task: Task, id: String)

  // ----- MANAGER, IR MANAGER, BROKER, WORKER ----- //

  /** Message Ready
    *
    * Inform the reducer that the manager, the broker or the worker is ready to
    * work.
    */
  case object Ready

  /** Message KillOk
    *
    * Manager acknowledgment for the Kill message.
    *
    * @param managerData data produced by the manager during the run
    */
  case class ManagerKillOk(managerData: ManagerData)

  /** Message IrManagerKillOk
    *
    * IR manager acknowledgment for the Kill message.
    */
  case object IrManagerKillOk

  /** Message BrokerKillOk
    *
    * Broker acknowledgment for the Kill message.
    *
    * @param brokerData data produced by the broker during the run
    */
  case class BrokerKillOk(brokerData: BrokerData)

  /** Message ForemanKillOk
    *
    * Foreman acknowledgment for the Kill message.
    *
    * @param performedOwnership ownership rate of the performed tasks
    */
  case class ForemanKillOk(performedOwnership: mutable.Map[Double, Int])

  /** Message WorkerKillOk
    *
    * Worker acknowledgment for the Kill message.
    */
  case class WorkerKillOk(workerData: WorkerData)

}

/** Represent a reducer that can adapt its workload by communicating with other
  * reducers of the map reduce the system.
  *
  * @param reduceJob reduce job to execute
  * @param rfh       remote file handler agent which deals with this reducer
  */
class AdaptiveReducer(
  reduceJob: ReduceJob,
  rfh: ActorRef
) extends Actor with Stash with utils.debugs.Debug {

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-reducer"))

  import AdaptiveReducer._

  // Acquaintances network of the reducer
  private var hasAcq: Boolean = false
  private var acquaintances: Set[ActorRef] = _
  private var rfhMap: Map[ActorRef, ActorRef] = _

  // Sub agents
  private var manager: ActorRef = _
  private var irManager: ActorRef = _
  private var broker: ActorRef = _
  private var foreman: ActorRef = _
  private var worker: ActorRef = _

  // Data run relative
  private var managerData: ManagerData = _
  private var brokerData: BrokerData = _
  private var workerData: WorkerData = _
  private var foremanData: Map[Double, Int] = _

  // Manager class if the graphical monitor is requested
  private class ManagerWithMonitor(
    broker: ActorRef,
    foreman: ActorRef,
    worker: ActorRef,
    taskBundle: TaskBundle,
    taskSplitPermission: TaskSplitPermission,
    acquaintances: Set[ActorRef],
    rfhMap: Map[ActorRef, ActorRef]
  ) extends Manager(
    broker,
    foreman,
    worker,
    taskBundle,
    taskSplitPermission,
    acquaintances,
    rfhMap
  ) with utils.taskMonitor.TaskHandler

  // Kill the reducer and send data to the monitor
  private def kill(): Unit = {
    val reducerData = new ReducerData(
      self.path.name,
      this.managerData,
      this.brokerData,
      this.workerData,
      this.foremanData
    )

    debugSend("Monitor.KillOk", "waitProperKill")
    context.parent ! Monitor.KillOk(reducerData)
    context stop self
  }

  def receive: Receive = this.rfhBroadcast

  /** State in which the reducer waits information about RFH and its peers. */
  def rfhBroadcast: Receive = {

    case GetRFH =>
      debugReceive("GetRFH from monitor", sender, "rfhBroadcast")
      debugSend("GetRFH", "rfhBroadcast")
      sender ! Monitor.GetRFH(this.rfh)

    case BroadcastRFH(map) =>
      debugReceive("BroadcastRFH", sender, "rfhBroadcast")
      this.rfhMap = map
      debugSend("Monitor.BroadcastRFHOk", "rfhBroadcast")
      sender ! Monitor.BroadcastRFHOk
      context become (
        this.waitTasks(Nil, 1)
        orElse this.handleDeprecatedWorkOn("waitTasks", 1)
        orElse this.handleDeprecatedAcquaintances("waitTasks")
        orElse this.handleDeprecatedGetRFH("waitTasks")
        orElse this.handleDeprecatedBroadcastRFH("waitTasks")
        orElse this.handleNotReady("waitTasks")
      )

  }

  /** State in which the reducer waits the mappers to deliver their results.
    *
    * @param tasks current tasks to perform
    */
  def waitTasks(
    tasks: List[Task],
    index: Int
  ): Receive = {

    // Receive its acquaintances network
    case Acquaintances(acq) if !this.hasAcq =>
      debugReceive("Acquaintances", sender, "waitTasks")
      sender ! Monitor.AcquaintancesOk
      this.acquaintances = acq
      this.hasAcq = true

    case WorkOn(receivedTasks, i) if i == index =>
      debugReceive("WorkOn(" + i + ")", sender, "waitTasks")
      debugSend("Partitioner.WorkOnOk(" + i + ")", "waitTasks")
      sender ! Partitioner.WorkOnOk(i)

      val newIndex = index + 1

      context become (
        this.waitTasks(tasks ++ receivedTasks, newIndex)
        orElse this.handleDeprecatedWorkOn("waitTasks", newIndex)
        orElse this.handleDeprecatedAcquaintances("waitTasks")
        orElse this.handleNotReady("waitTasks")
      )

    case Execute =>
      debugReceive("Execute", sender, "waitTasks")
      debugSend("Monitor.ExecuteOk", "waitTasks")
      context.parent ! Monitor.ExecuteOk

      val worker = context.actorOf(
        Props(
          classOf[Worker],
          this.reduceJob,
          this.rfh
        ),
        "worker@" + self.path.name
      )
//      val worker = context.actorOf(
//        Props(
//          classOf[TimeWorker],
//          { task: Task => task.absoluteCost / 10 },
//          this.rfhMap
//        ),
//        "worker@" + self.path.name
//      )
      val broker = context.actorOf(
        Props(classOf[Broker], this.acquaintances.size, this.rfhMap),
        "broker@" + self.path.name
      )
      val foreman = context.actorOf(
        Props(classOf[Foreman], worker, this.rfh),
        "foreman@" + self.path.name
      )
      val taskBundle = this.config.taskBundle(
        tasks,
        self,
        this.rfhMap
      )
      val manager = if (this.config.taskMonitor) {
        // task monitor enables
        context.actorOf(
          Props(
            classOf[ManagerWithMonitor],
            tasks,
            broker,
            foreman,
            worker,
            taskBundle,
            if (this.config.withTaskSplit) TaskSplitAllowed else TaskSplitDenied,
            this.acquaintances,
            this.rfhMap
          ).withDispatcher("custom-dispatcher"),
          "manager@" + self.path.name
        )
      } else {
        context.actorOf(
          Props(
            classOf[Manager],
            broker,
            foreman,
            worker,
            taskBundle,
            if (this.config.withTaskSplit) TaskSplitAllowed else TaskSplitDenied,
            this.acquaintances,
            this.rfhMap
          ).withDispatcher("custom-dispatcher"),
          "manager@" + self.path.name
        )
      }
      val irManager = context.actorOf(
        Props(classOf[IrManager], this.reduceJob, manager, this.rfh),
        "irManager@" + self.path.name
      )

      this.manager = manager
      this.irManager = irManager
      this.broker = broker
      this.foreman = foreman
      this.worker = worker

      // Inform TaskHandler listeners that this reducer has been initialized
      // with given data
      if (this.config.taskMonitor) {
        self ! utils.taskMonitor.TaskEventHandler.TaskInitialized(tasks)
      }

      unstashAll()
      context become (
        this.waitReady(5)
        orElse this.handleDeprecatedExecute("waitReady")
        orElse this.handleNotReady("waitReady")
      )

  }

  /** State in which the reducer waits its sub-agents.
    *
    * @param nbReady number of Ready message to wait
    */
  def waitReady(nbReady: Int): Receive = {

    case Ready =>
      debugReceive("Ready", sender, "waitReady")

      val newNbReady = nbReady - 1

      if (newNbReady == 0) {
        debugSend("Go (manager)", "waitReady")
        this.manager ! Manager.Go
        debugSend("Go (IR manager)", "waitReady")
        this.irManager ! IrManager.Go
        debugSend("Go (broker)", "waitReady")
        this.broker ! Broker.Go
        debugSend("Go (foreman)", "waitReady")
        this.foreman ! Foreman.Go
        debugSend("Go (worker)", "waitReady")
        this.worker ! Worker.Go
        unstashAll()
        context become (
          this.active
          orElse this.handleDeprecatedExecute("active")
          orElse this.handleUnexpected("active")
        )
      } else {
        context become (
          this.waitReady(newNbReady)
          orElse this.handleDeprecatedExecute("waitReady")
          orElse this.handleNotReady("waitReady")
        )
      }

    case msg@Manager.InitialContribution(_, _) =>
      if (sender == this.manager) {
        this.acquaintances foreach { _ ! msg }
      } else {
        this.manager ! msg
      }

  }

  /** In this state the reducer redirect the message for and from its
    * sub-agents.
    */
  def active: Receive = {

    case msg@Broker.CFP(ref, cost, contribution, id) =>
      debugReceive("Broker.CFP for CFP " + id, sender, "active")

      if (ref == this.broker) {
        this.acquaintances foreach {
          _ ! Broker.CFP(self, cost, contribution, id)
        }

        debugSend(
          "Broker.CFP for CFP " + id + " to " + this.acquaintances.size
            + " acquaintances",
          "active"
        )
      } else {
        this.broker ! msg
        debugSend("Broker.CFP for CFP " + id + " to the broker", "active")
      }

    case Broker.Accept(ref, task, id) =>
      debugReceive("Broker.Accept for CFP " + id, sender, "active")
      ref ! AdaptiveReducer.Accept(task, id)
      debugSend(
        "AdaptiveReducer.Accept to " + ref + " for CFP id " + id, "active"
      )

    case Accept(task, id) =>
      debugReceive("Accept for CFP " + id, sender, "active")
      this.broker ! Broker.Accept(self, task, id)
      debugSend("Broker.Accept to the broker for CFP id " + id, "active")

    case msg@Broker.Reject(ref, id) =>
      debugReceive("Broker.Reject for CFP " + id, sender, "active")

      if (sender == this.broker) {
        ref ! Broker.Reject(self, id)
      } else {
        this.broker ! msg
      }

    case msg@Broker.Confirm(ref, id) =>
      debugReceive("Broker.Confirm for CFP " + id, sender, "active")

      if (sender == this.broker) {
        ref ! Broker.Confirm(self, id)
      } else {
        this.broker ! msg
      }

    case msg@Broker.Decline(ref, id, contrib) =>
      debugReceive("Broker.Decline for CFP " + id, sender, "active")

      if (sender == this.broker) {
        debugSend(
          "Broker.Decline to" + ref.path.name + " for CFP " + id, "active"
        )
        ref ! Broker.Decline(self, id, contrib)
      } else {
        debugSend(
          "Broker.Decline to" + broker.path.name + " for CFP " + id, "active"
        )
        this.broker ! msg
      }

    case msg@Broker.Propose(ref, contributionValue, id) =>
      debugReceive(
        "Broker.Propose for CFP " + id + " with contribution "
          + contributionValue,
        sender,
        "active"
      )

      if (sender == this.broker) {
        debugSend(
          "Broker.Propose to" + ref.path.name + " for CFP " + id
            + " with contribution " + contributionValue,
          "active"
        )
        ref ! Broker.Propose(self, contributionValue, id)
      } else {
        debugSend(
          "Broker.Propose to" + this.broker.path.name + " for CFP " + id
            + " with contribution " + contributionValue,
          "active"
        )
        this.broker ! msg
      }

    case msg@Manager.InformContribution(ref, contribution) =>
      if (ref == this.manager) {
        // Informs all acquaintances about my new contribution
        debugSend("Manager.InformContribution to all acquaintances", "active")
        this.acquaintances foreach {
          _ ! Manager.InformContribution(self, contribution)
        }
      } else {
        debugSend("Manager.InformContribution to the manager", "active")
        this.manager ! msg
      }

    case msg@Monitor.ReducerIdle(_, _) => context.parent ! msg

    case msg@Manager.ReducerIdleOk => this.manager ! msg

    case msg@Monitor.ReducerActive(_) => context.parent ! msg

    case msg@Manager.ReducerActiveOk => this.manager ! msg

    case msg@IrManager.GetIrResult(irResult) =>
      if (sender == this.worker) {
        debugSend("IR result to " + irResult.finalPerformer, "active")
        irResult.finalPerformer ! msg
      } else {
        this.irManager ! msg
      }

    case msg@Manager.ReadyToDie(_) =>
      this.manager ! msg

    case msg@Monitor.ReadyToDieAnswer(_, _) => context.parent ! msg

    case Kill =>
      debugReceive("Kill", sender, "active")
      context become (
        this.waitProperKill(5)
        orElse this.handleDeprecatedKill("waitProperKill")
        orElse this.handleUnexpected("waitProperKill")
      )
      this.manager ! AdaptiveReducer.Kill
      this.irManager ! AdaptiveReducer.Kill
      this.broker ! AdaptiveReducer.Kill
      this.foreman ! AdaptiveReducer.Kill
      this.worker ! AdaptiveReducer.Kill

  }

  /** State in which the reducer waits its sub-agents before stopping itself.
    *
    * @param agentsToWait number of sub-agents to wait before killing itself
    */
  def waitProperKill(agentsToWait: Int): Receive = {

    case ManagerKillOk(receivedManagerData) =>
      debugReceive("ManagerKillOk", sender, "waitProperKill")
      this.managerData = receivedManagerData

      val newAgentsToWait = agentsToWait - 1

      if (newAgentsToWait == 0) {
        this.kill()
      } else {
        context become (
          this.waitProperKill(agentsToWait - 1)
          orElse this.handleDeprecatedKill("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

    case BrokerKillOk(receivedBrokerData) =>
      debugReceive("BrokerKillOk", sender, "waitProperKill")
      this.brokerData = receivedBrokerData

      val newAgentsToWait = agentsToWait - 1

      if (newAgentsToWait == 0) {
        this.kill()
      } else {
        context become (
          this.waitProperKill(newAgentsToWait)
          orElse this.handleDeprecatedKill("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

    case WorkerKillOk(receivedWorkerData) =>
      debugReceive("WorkerKillOk", sender, "waitProperKill")
      this.workerData = receivedWorkerData

      val newAgentsToWait = agentsToWait - 1

      if (newAgentsToWait == 0) {
        this.kill()
      } else {
        context become (
          this.waitProperKill(newAgentsToWait)
          orElse this.handleDeprecatedKill("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

    case IrManagerKillOk =>
      debugReceive("IrManagerKillOk", sender, "waitProperKill")

      val newAgentsToWait = agentsToWait - 1

      if (newAgentsToWait == 0) {
        this.kill()
      } else {
        context become (
          this.waitProperKill(newAgentsToWait)
          orElse this.handleDeprecatedKill("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

    case ForemanKillOk(receivedForemanData) =>
      debugReceive("ForemanKillOk", sender, "waitProperKill")
      this.foremanData = receivedForemanData.toMap

      val newAgentsToWait = agentsToWait - 1

      if (newAgentsToWait == 0) {
        this.kill()
      } else {
        context become (
          this.waitProperKill(newAgentsToWait)
          orElse this.handleDeprecatedKill("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

  }

  /** Handle the deprecated Acquaintances messages.
    *
    * @param state state in which the deprecated Acquaintances message is
    *              received
    */
  def handleDeprecatedAcquaintances(state: String): Receive = {

    case Acquaintances(_) =>
      debugDeprecated("Acquaintances", sender, state)
      sender ! Monitor.AcquaintancesOk

  }

  /** Handle the deprecated GetRFH messages.
    *
    * @param state state in which the deprecated GetRFH message is received
    */
  def handleDeprecatedGetRFH(state: String): Receive = {

    case GetRFH =>
      debugDeprecated("GetRFH", sender, state)
      debugSend("GetRFH", state)
      sender ! Monitor.GetRFH(this.rfh)

  }

  /** Handle the deprecated BroadcastRFH messages.
    *
    * @param state state in which the deprecated BroadcastRFH message is
    *              received
    */
  def handleDeprecatedBroadcastRFH(state: String): Receive = {

    case BroadcastRFH(_) =>
      debugDeprecated("BroadcastRFH", sender, state)
      debugSend("BroadcastRFHOk", state)
      sender ! Monitor.BroadcastRFHOk

  }

  /** Handle the early messages.
    *
    * @param state state in which the early message is received
    */
  def handleNotReady(state: String): Receive = {

    case msg@_ =>
      debug("stash " + msg + " because not ready to handle it")
      stash()

  }

  /** Handle the deprecated WorkOn messages.
    *
    * @param state state in which the deprecated WorkOn messages is received
    * @param index current index of the exchange with the partitioner
    */
  def handleDeprecatedWorkOn(state: String, index: Int): Receive = {

    case WorkOn(_, i) if i < index =>
      debugDeprecated("WorkOn(" + i + ")", sender, state)
      debugSend("Partitioner.WorkOnOk", state)
      sender ! Partitioner.WorkOnOk(i)

  }

  /** Handle the deprecated Execute messages.
    *
    * @param state state in which the deprecated Execute message is received
    */
  def handleDeprecatedExecute(state: String): Receive = {

    case Execute =>
      debugDeprecated("Execute", sender, state)
      debugSend("ExecuteOk", state)
      context.parent ! Monitor.ExecuteOk

  }

  /** Handle the deprecated Kill messages.
    *
    * @param state state in which the deprecated Kill message is received
    */
  def handleDeprecatedKill(state: String): Receive = {

    case Kill =>
      debugDeprecated("Kill", sender, "waitProperKill")

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}

/** Adaptive reducer when the graphical monitor is requested. */
class AdaptiveReducerWithMonitor(
  reduceJob: ReduceJob,
  rfh: ActorRef
) extends AdaptiveReducer(
  reduceJob,
  rfh
) with utils.taskMonitor.TaskEventHandler
