package mapreduce.adaptive

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import akka.dispatch.RequiresMessageQueue
import akka.pattern.ask
import akka.util.Timeout
import mapreduce.time.Timer
import utils.bundles.{KEligibleTaskBundle, TaskBundle}
import utils.config.ConfigurationBuilder
import utils.experiments.{Archive, ManagerData}
import utils.files.FileWriter
import utils.mailboxes.ManagerMessageQueueSemantics
import utils.strategies.split.TaskSplitPermission
import utils.tasks._

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._

/** Companion object of the Manager class.
  *
  * Contain all the messages that a manager could receive.
  */
object Manager {

  // ---------- FROM REDUCER ---------- //

  /** Message Go
    *
    * Inform the manager that it can start to work.
    */
  object Go

  /** Message ReducerIdleOk
    *
    * Acknowledgment for the Monitor.ReducerIdle message.
    */
  case object ReducerIdleOk

  /** Message ReducerActiveOk
    *
    * Acknowledgment for the Monitor.ReducerActive message.
    */
  case object ReducerActiveOk

  /** Message ReadyToDie
    *
    * Request from the monitor to ask if the manager is ready to die.
    *
    * @param id identifier of the attempt to end the run
    */
  case class ReadyToDie(id: Int)

  // ---------- FROM FOREMAN ---------- //

  /** Message WorkerDone
    *
    * Tell to the manager that the worker ends to perform a task.
    *
    * @param performedCost     cost of the task that the worker just performed
    * @param nbPerformedValues number of values of the task that the worker just
    *                          performed
    */
  case class WorkerDone(performedCost: Long, nbPerformedValues: Long)

  /** Message CurrentTask
    *
    * Part of the current task which is not already performed from the foreman.
    *
    * @param task given back task from the foreman
    */
  case class CurrentTask(task: Task)

  /** Message CurrentTaskPerformed
    *
    * Inform the manager that the current task is totally performed.
    */
  object CurrentTaskPerformed

  // ---------- FROM WORKER ---------- //

  /** Message Remaining
    *
    * Informs manager about the remaining cost of the worker current task.
    *
    * @param remains remaining cost of the worker current task
    */
  case class Remaining(remains: Long)

  // --------- FROM BROKER ---------- //

  /** Message BrokerNotBusy
    *
    * Tell to the manager that the broker is not busy.
    */
  object BrokerNotBusy

  /** Message BrokerFinish
    *
    * Tell to the manager that the broker has delegated a task.
    */
  object BrokerFinish

  /** Message QueryContribution
    *
    * Ask the contribution to the manager.
    */
  object QueryContribution

  /** Message Request
    *
    * Ask to the manager to add a task in the tasks bundle.
    *
    * @param task requested task
    */
  case class Request(task: Task)

  /** Message RequestAndNotBusy
    *
    * Ask the manager to add a task in the task bundle and inform the manager
    * that the broker is not busy.
    *
    * @param task requested task
    */
  case class RequestAndNotBusy(task: Task)

  /** Message BrokerDeny
    *
    * Tell to the manager that the broker deny a request (no proposal).
    */
  object BrokerDeny

  /** Message CFPDeclineByAll
    *
    * Tells to the manager that a CFP has been declined by all acquaintance.
    *
    * @param idCFP identifier of declined CFP
    */
  case class CFPDeclinedByAll(idCFP: String)

  /** Message BrokerReady
    *
    * Tell to the manager that the broker has find an other reducer to perform
    * the task.
    *
    * @param task task the broker wish to give to another reducer
    */
  case class BrokerReady(task: Task)

  /** Message InformContribution
    *
    * Informs the manager about a new known contribution value.
    *
    * @param acquaintance the acquaintance (reducer) whose contribution value is
    *                     known
    * @param contribution the contribution value
    */
  case class InformContribution(acquaintance: ActorRef, contribution: Long)

  // ---------- FROM OTHER MANAGERS ---------- //

  /** Message InitialContribution
    *
    * Informs the manager from an other manager initial contribution.
    *
    * @param acquaintance the acquaintance which share its contribution
    * @param contribution the acquaintance's contribution
    */
  case class InitialContribution(acquaintance: ActorRef, contribution: Long)

}

/** Represent the manager of a adaptive reducer.
  *
  * A manager manage the negotiation with the other adaptive reducers and
  * handle the global amount of work of the adaptive reducer.
  *
  * @param broker              broker with which the manager has to deal
  * @param foreman             foreman with which the manager has to deal
  * @param worker              worker with which the manager has to deal
  * @param taskBundle          task bundle of the manager
  * @param taskSplitPermission task split permission of the manager
  * @param acquaintances       acquaintances of the manager's reducer
  * @param rfhMap              map which associates a reducer to its RFH
  */
class Manager(
  broker: ActorRef,
  foreman: ActorRef,
  worker: ActorRef,
  taskBundle: TaskBundle,
  taskSplitPermission: TaskSplitPermission,
  acquaintances: Set[ActorRef],
  rfhMap: Map[ActorRef, ActorRef]
) extends Actor with RequiresMessageQueue[ManagerMessageQueueSemantics]
                with utils.debugs.Debug {

  import Manager._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-manager"))

  // Initialization messages
  debug("sends its ref to the broker")
  this.broker ! Broker.ManagerRef(self)

  // Save of the split keys currently being performed by other reducers
  private var splitKeys: Set[String] = Set[String]()

  // true iff the broker is busy
  private var brokerBusy: Boolean = false

  // true iff the worker is busy
  private var workerBusy: Boolean = false

  // Archive to records performed tasks number of values
  private val nbValuesArchive: Archive[(String, Long), Long] =
    new Archive[(String, Long), Long](self.path.name + "_nbValues")

  // List of number of values of performed tasks.
  private var nbValuesPerformed: List[Long] = List()

  // List of performed costs
  private var performedCosts: List[Long] = List()

  // Count the number of tasks done by the worker (task monitor related)
  private var taskDone: Int = 0

  // Archive to record the task splits
  private val taskSplitArchive: Archive[Int, Int] =
    new Archive[Int, Int](self.path.name + "_splits")

  // Count the number of task split
  private var taskSplitCount: Int = 0

  // Archive to record when the manager is in pause state or not
  private val pauseArchive: Archive[(String, Boolean), Int] =
    new Archive[(String, Boolean), Int](self.path.name + "_pause")

  // Archive to record when the manager is in pause state or not
  private val idleArchive: Archive[(String, Boolean), Int] =
    new Archive[(String, Boolean), Int](self.path.name + "_idle")

  // Count the number of pause state switch
  private val pauseSwitchCount: Int = 0

  // Archive to record contribution fluctuation
  private val contributionArchive: Archive[(String, Long), Long] =
    new Archive[(String, Long), Long](self.path.name + "_contribution")

  this.contributionArchive.addRecord(
    self.path.name,
    this.taskBundle.getOwnerWorkload
  )

  // Stores the last known contribution of acquaintances.
  private val contributionMap: mutable.Map[ActorRef, Long] = {
    val acquaintancesCouples = this.acquaintances map {
      acc => (acc, 0.toLong)
    }

    mutable.Map.empty[ActorRef, Long] ++ acquaintancesCouples
  }

  // Add a record in the pause archive to specify that the manager is in pause
  // state
  private def addPauseRecord(): Unit =
    this.pauseArchive.addRecord(self.path.name, true)

  // Add a record in the idle archive to specify that the manager is in idle
  // state
  private def addIdleRecord(): Unit =
    this.idleArchive.addRecord(self.path.name, true)

  // Add a record in the pause archive to specify that the manager is active
  private def addActiveRecord(): Unit = {
    this.pauseArchive.addRecord(self.path.name, false)
    this.idleArchive.addRecord(self.path.name, false)
  }

  /** Returns contribution as the sum of the costs of the not yet treated tasks
    * and the remaining cost of the task-in-progress by the worker.
    *
    * @return the contribution of this reducer
    */
  private def getContribution: Long = {
    implicit val timeout: Timeout = Timeout(Duration(2000, "millis"))

    debugSend("QueryRemainingWork[synchronous] to foreman", "active")

    val future = this.foreman ? Foreman.QueryRemainingWork
    val taskInProgressCost = {
      val remainingMsg = try {
        Await.result(future, timeout.duration).asInstanceOf[Remaining]
      } catch {
        case _: Exception => Remaining(0)
      }

      remainingMsg.remains
    }

    this.taskBundle.getOwnerWorkload + taskInProgressCost
  }

  /** Give (send) to the worker a task to process.
    *
    * @param taskToProcess the task given to the worker
    * @param state         state of this manager when it gives the task
    */
  def giveToWorker(taskToProcess: Task, state: String): Unit = {
    debugSend("Foreman.Perform", state)
    this.foreman ! Foreman.Perform(taskToProcess)

    // If the task is an FR task and contains several chunks, the foreman will
    // split it in order to give it to the worker
    if (taskToProcess.isFR && taskToProcess.canBeDivided) {
      this.splitKeys = this.splitKeys + taskToProcess.key
    }
  }

  /** Give a task to delegate to the broker.
    *
    * @param taskToDelegate task to delegate
    * @param contribution   current contribution of the reducer
    * @param state          state of the manager when it gives the task to the
    *                       broker
    */
  def giveToBroker(
    taskToDelegate: Task,
    contribution: Long,
    state: String
  ): Unit = {
    debugSend("Broker.Submit for key " + taskToDelegate.key, state)
    this.broker ! Broker.Submit(taskToDelegate, contribution)
  }

  /** Defines the operation to do when the worker has finished a task. */
  def workerHasDone(): Unit = {
    this.contributionArchive.addRecord(
      self.path.name,
      this.taskBundle.getOwnerWorkload
    )
    this.taskDone += 1
  }

  // Task split process
  private def taskSplitProcess(ownContribution: Long): Boolean = {
    val maybeKey = this.taskBundle.applySplitProcess(
      ownContribution,
      this.contributionMap
    )

    if (maybeKey.isDefined) {
      this.splitKeys = this.splitKeys + maybeKey.get
      this.taskSplitArchive addRecord 1
      this.taskSplitCount += 1
      // Returns true to indicate that the task split process produced new tasks
      true
    } else {
      // Returns false to indicate that the task split process did not produce
      // new tasks
      false
    }
  }

  // Find a task to delegate or split a task
  private def delegateOrSplit(
    state: String,
    ownContribution: Long = this.getContribution
  ): (Long, Option[Task]) = {
    val taskToDelegate = this.taskBundle.nextTaskToDelegate(
      ownContribution,
      this.contributionMap
    )

    // There is a task to delegate
    if (taskToDelegate.isDefined) {
      (ownContribution, taskToDelegate)
    }
    // No task to delegate and the reducer is the most loaded, the manager
    // tries to split its biggest task
    else if (this.taskSplitPermission.canInitiateSplit(ownContribution)) {
      val newTasks = this.taskSplitProcess(ownContribution)

      if (newTasks) {
        val otherTaskToDelegate = this.taskBundle.nextTaskToDelegate(
          ownContribution,
          this.contributionMap
        )

        if (otherTaskToDelegate.isDefined) {
          (ownContribution, otherTaskToDelegate)
        } else {
          (ownContribution, None)
        }
      } else {
        (ownContribution, None)
      }
    }
    // No task to delegate
    else {
      (ownContribution, None)
    }
  }

  private def actionWhenActiveBrokerNotBusy(state: String): Unit =
    // The task bundle is not empty, the broker can try to delegate an other
    // task
    if (this.taskBundle.nonEmpty) {
      val (contribution, taskToDelegate) = this.delegateOrSplit(state)

      // There is a task to delegate, the manager send this task to the broker
      if (taskToDelegate.isDefined) {
        this.giveToBroker(taskToDelegate.get, contribution, state)
      }
      // There is no task to delegate, but the task bundle is not empty, the
      // manager switches in pause state
      else {
        this.brokerBusy = false
        this.addPauseRecord()
        context become (
          this.pause
          orElse this.handleDeprecatedReducerIdleOk("pause")
          orElse this.handleDeprecatedReducerActiveOk("pause")
          orElse this.handleDeprecatedTimeout("pause")
          orElse this.handleUnexpected("pause")
        )
      }
    }
    // The task bundle is empty and the worker is busy, the manager request the
    // current task to the foreman
    else if (this.workerBusy) {
      this.brokerBusy = false
      this.foreman ! Foreman.GetCurrentTask
      this.addActiveRecord()
      context become (
        this.active
        orElse this.waitingForemanTask
        orElse this.handleUnexpected("active")
      )
    }
    // The task bundle is empty and the worker is not busy but it remains split
    // task to get back, the manager switches in pause state
    else if (this.splitKeys.nonEmpty) {
      this.brokerBusy = false
      this.addPauseRecord()
      context become (
        this.pause
        orElse this.handleDeprecatedReducerIdleOk("pause")
        orElse this.handleDeprecatedReducerActiveOk("pause")
        orElse this.handleDeprecatedTimeout("pause")
        orElse this.handleUnexpected("pause")
      )
    }
    // The task bundle is empty, the worker is not busy and there is no more
    // split task to wait, the manager switches in idle state
    else {
      this.brokerBusy = false
      this.addIdleRecord()
      this goToIdleState state
    }

  // Behavior of the manager when it receives a Request message
  private def processRequestMessage(
    state: String,
    requestedTask: Task
  ): Unit = {
    // The message come from the broker
    if (sender == this.broker) {
      debugReceive("Request", sender, state)
    }
    // The message come from the IR manager
    else {
      debugReceive("Request from the IR manager", sender, state)
      // Remove the key to the split keys list
      this.splitKeys = this.splitKeys - requestedTask.key
    }
    // Add the new task to the bundle
    this.taskBundle.addTask(requestedTask)

    // The worker is not busy, the bundle was empty
    // Now that the bundle contains one task, the manager can give it to the
    // worker
    if (!this.workerBusy) {
      val ownContribution = this.taskBundle.ownerWorkload
      val taskToPerform = this.taskBundle.nextTaskToPerform(
        ownContribution,
        this.contributionMap
      ).get

      this.taskBundle.removeTask(taskToPerform)
      this.workerBusy = true
      this.giveToWorker(taskToPerform, state)
    }
  }

  // Pass in idle state.
  private def goToIdleState(state: String): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.idle(timer)
      orElse this.handleDeprecatedReducerActiveOk("idle")
      orElse this.handleDeprecatedTimeout("idle")
      orElse this.handleUnexpected("idle")
    )
    debugSend("Monitor.ReducerIdle", state)
    context.parent ! Monitor.ReducerIdle(
      context.parent.path.name,
      System.currentTimeMillis()
    )
    timer ! Timer.Start
  }

  // Change state from idle
  private def goToStateWaitingReducerActiveOk(
    previousStateName: String,
    newStateName: String,
    newState: Receive
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      newState
      orElse this.waitingReducerActiveOk(newStateName, newState, timer)
      orElse this.handleDeprecatedReducerIdleOk(newStateName)
      orElse this.handleDeprecatedTimeout(newStateName)
      orElse this.handleUnexpected(newStateName)
    )
    debugSend("Monitor.ReducerActive", previousStateName)
    context.parent ! Monitor.ReducerActive(context.parent.path.name)
    timer ! Timer.Start
  }

  // Sends its initial contribution to the other reducers
  context.parent ! InitialContribution(
    context.parent,
    this.taskBundle.ownerWorkload
  )

  def receive: Receive = if (this.acquaintances.nonEmpty) {
    this.waitContribution(this.acquaintances) orElse
      this.handleUnexpected("waitContribution")
  } else {
    debug("sends ready to the reducer")
    context.parent ! AdaptiveReducer.Ready
    this.waitGo orElse this.handleUnexpected("waitGo")
  }

  /** State in which the manager waits for the initial contribution of all the
    * other reducers.
    */
  def waitContribution(toWait: Set[ActorRef]): Receive = {

    case InitialContribution(acq, contribution) if toWait contains acq  =>
      debugReceive("InitialContribution", sender, "waitContribution")
      this.contributionMap.update(acq, contribution)

      val newToWait = toWait - acq

      if (newToWait.isEmpty) {
        debug("sends ready to the reducer")
        context.parent ! AdaptiveReducer.Ready
        context become (this.waitGo orElse this.handleUnexpected("waitGo"))
      } else {
        context become (
          this.waitContribution(newToWait) orElse
          this.handleUnexpected("waitContribution")
        )
      }

  }

  /** State in which the manager wait the green light of the reducer. */
  def waitGo: Receive = {

    case Go =>
      debugReceive("Go from the reducer", sender, "waitGo")

      if (this.taskBundle.isEmpty) {
        debugSend("ReducerIdle", "waitGo")
        // Informs monitor that this is idle since all managers are assumed
        // active at beginning
        this.addIdleRecord()
        this goToIdleState "waitGo"
      } else {
        // No need to inform monitor : all managers are assumed active at
        // beginning

        // The bundle is not empty, the worker has to process a task
        val ownContribution = this.taskBundle.ownerWorkload

        // If the task bundle contains only one task, the manager tries first to
        // split it in order to be able to give one task to the worker and one
        // task to the broker
        if (this.taskBundle.size == 1 && this.config.withTaskSplit) {
          this.taskSplitProcess(ownContribution)
        }

        val taskToPerform = this.taskBundle.nextTaskToPerform(
          ownContribution,
          this.contributionMap
        ).get

        debug("Initial contribution: " + ownContribution)
        this.taskBundle.removeTask(taskToPerform)
        this.workerBusy = true
        this.giveToWorker(taskToPerform, "active")

        // Now that the worker is busy, maybe the bundle still contains a task
        // to submit
        if (this.taskBundle.nonEmpty) {
          val (_, taskToDelegate) =
            this.delegateOrSplit("waitGo", ownContribution)

          // There is a task to delegate
          if (taskToDelegate.isDefined) {
            val task = taskToDelegate.get

            this.brokerBusy = true
            this.addActiveRecord()
            context become (
              this.active
                orElse this.handleDeprecatedReducerIdleOk("active")
                orElse this.handleDeprecatedReducerActiveOk("active")
                orElse this.handleDeprecatedTimeout("active")
                orElse this.handleUnexpected("active")
            )
            debugSend("Broker.Submit for key " + task.key, "active")
            this.broker ! Broker.Submit(task, ownContribution)
          }
          // No task to delegate, the manager switches in pause state
          else {
            this.brokerBusy = false
            this.addPauseRecord()
            context become (
              this.pause
                orElse this.handleDeprecatedReducerIdleOk("pause")
                orElse this.handleDeprecatedReducerActiveOk("pause")
                orElse this.handleDeprecatedTimeout("pause")
                orElse this.handleUnexpected("pause")
            )
          }
        }
        // No task to delegate, the manager switches in pause state
        else {
          this.brokerBusy = false
          this.addPauseRecord()
          context become (
            this.pause
            orElse this.handleDeprecatedReducerIdleOk("pause")
            orElse this.handleDeprecatedReducerActiveOk("pause")
            orElse this.handleDeprecatedTimeout("pause")
            orElse this.handleUnexpected("pause")
          )
        }
      }

  }

  /** Active state of a manager. */
  def active: Receive = {

    // ---------- WORKER ----------- //

    case WorkerDone(performedCost, nbPerformedValues) =>
      debugReceive("WorkerDone", sender, "active")
      this.workerHasDone()
      this.nbValuesArchive.addRecord(
        self.path.name,
        nbPerformedValues
      )
      this.nbValuesPerformed = nbPerformedValues :: this.nbValuesPerformed
      this.performedCosts = performedCost :: this.performedCosts

      // Worker has just finished a task  then it is useless to require about
      // work in progress
      val ownContribution = this.taskBundle.ownerWorkload

      // Informs all acquaintances about my new contribution
      debugSend("InformContribution(" + ownContribution + ")" , "active")
      context.parent ! InformContribution(self, ownContribution)

      // It remains tasks to give to the worker
      if (this.taskBundle.nonEmpty) {
        val taskToPerform = this.taskBundle.nextTaskToPerform(
          ownContribution,
          this.contributionMap
        ).get

        this.taskBundle.removeTask(taskToPerform)
        this.giveToWorker(taskToPerform, "active")
      }
      // The task bundle is empty, but it remains split keys, the manager
      // switches in pause state
      else if (this.splitKeys.nonEmpty) {
        this.workerBusy = false
        context become (
          this.pause
            orElse this.handleDeprecatedReducerIdleOk("pause")
            orElse this.handleDeprecatedReducerActiveOk("pause")
            orElse this.handleDeprecatedTimeout("pause")
            orElse this.handleUnexpected("pause")
        )
      }
      // The task bundle is empty and there is no split keys, the manager
      // switches in idle state
      else {
        this.workerBusy = false
        debug("becomes idle")
        this.addIdleRecord()
        this goToIdleState "active"
      }

    // ---------- BROKER ---------- //

    case BrokerNotBusy =>
      debugReceive("BrokerNotBusy", sender, "active")
      this actionWhenActiveBrokerNotBusy "active"

    case QueryContribution =>
      debugReceive("QueryContribution", sender, "active")
      this.brokerBusy = true

      // Informs Broker on task list contribution value
      val contribution =
        if (this.workerBusy) {
          this.getContribution
        } else {
          this.taskBundle.ownerWorkload
        }

      this.contributionArchive.addRecord(
        self.path.name,
        contribution
      )
      debugSend("Inform ", "active")
      sender ! Broker.Inform(contribution)

    case BrokerFinish =>
      debugReceive("BrokerFinish", sender, "active")
      this actionWhenActiveBrokerNotBusy "active"

    case RequestAndNotBusy(requestedTask: Task) =>
      debugReceive("RequestAndNotBusy", sender, "active")
      this.taskBundle.addTask(requestedTask)

      // The worker is busy, the broker can try to delegate an other task
      if (this.workerBusy) {
        val (contribution, taskToDelegate) = this.delegateOrSplit("active")

        if (taskToDelegate.isDefined) {
          this.giveToBroker(taskToDelegate.get, contribution, "active")
        } else {
          this.brokerBusy = false
          this.addPauseRecord()
          context become (
            this.pause
              orElse this.handleDeprecatedReducerIdleOk("pause")
              orElse this.handleDeprecatedReducerActiveOk("pause")
              orElse this.handleDeprecatedTimeout("pause")
              orElse this.handleUnexpected("pause")
          )
        }
      }
      // The worker is not busy, the bundle was empty
      // Now that the bundle contains one task, the manager can give it to the
      // worker
      else {
        val ownContribution = this.taskBundle.ownerWorkload
        val taskToPerform = this.taskBundle.nextTaskToPerform(
          ownContribution,
          this.contributionMap
        ).get

        this.brokerBusy = false
        this.addPauseRecord()
        context become (
          this.pause
          orElse this.handleDeprecatedReducerIdleOk("pause")
          orElse this.handleDeprecatedReducerActiveOk("pause")
          orElse this.handleDeprecatedTimeout("pause")
          orElse this.handleUnexpected("pause")
        )
        this.taskBundle.removeTask(taskToPerform)
        this.workerBusy = true
        this.giveToWorker(taskToPerform, "active")
      }

    case Request(requestedTask: Task) =>
      this.processRequestMessage("active", requestedTask)

    case BrokerDeny =>
      debugReceive("BrokerDeny", sender, "active")
      this actionWhenActiveBrokerNotBusy "active"

    case CFPDeclinedByAll(idCFP) =>
      debugReceive("CFPDeclinedByAll " + idCFP, sender, "active")
      this.brokerBusy = false

      if (
        !this.workerBusy &&
        this.taskBundle.isEmpty &&
        this.splitKeys.isEmpty
      ) {
        this.addIdleRecord()
        this goToIdleState "active"
      } else {
        this.addPauseRecord()
        context become (
          this.pause
            orElse this.handleDeprecatedReducerIdleOk("pause")
            orElse this.handleDeprecatedReducerActiveOk("pause")
            orElse this.handleDeprecatedTimeout("pause")
            orElse this.handleUnexpected("pause")
        )
      }

    case BrokerReady(negotiatedTask) =>
      debugReceive("BrokerReady", sender, "active")

      // The task is in the bundle, the manager approves the delegation
      if (this.taskBundle.containsTask(negotiatedTask)) {
        this.taskBundle.removeTask(negotiatedTask)
        debugSend("Approve", "active")
        sender ! Broker.Approve
      }
      // The task is not in the bundle, the manager cancel the delegation
      else {
        debugSend("Cancel", "active")
        sender ! Broker.Cancel
        this actionWhenActiveBrokerNotBusy "active"
      }

    case InformContribution(initiator, contribution) =>
      debugReceive(
        "InformContribution (" + initiator.path.name + " -> " + contribution + ")",
        sender,
        "active"
      )
      this.contributionMap.update(initiator, contribution)

    // ---------- MONITOR ---------- //

    case ReadyToDie(id) =>
      debugReceive("ReadyToDie", sender, "active")
      debugSend("ReadyToDieAnswer(false)", "active")
      sender ! Monitor.ReadyToDieAnswer(id, ready = false)

  }

  def waitingReducerActiveOk(
    currentStateName: String,
    currentState: Receive,
    timer: ActorRef
  ): Receive = {

    // ---------- MONITOR ---------- //

    case ReducerActiveOk =>
      timer ! Timer.Cancel
      this.addActiveRecord()
      context become (
        this.active
        orElse this.handleDeprecatedReducerActiveOk("active")
        orElse this.handleDeprecatedReducerIdleOk("active")
        orElse this.handleDeprecatedTimeout("active")
        orElse this.handleUnexpected("active")
      )

    case ReadyToDie(id) =>
      debugReceive("ReadyToDie", sender, "waitingReducerActiveOk")
      debugSend("ReadyToDieAnswer(false)", "active")
      sender ! Monitor.ReadyToDieAnswer(id, ready = false)

    // ---------- TIMER ---------- //

    case Timer.Timeout if sender == timer =>
      this.goToStateWaitingReducerActiveOk(
        currentStateName,
        currentStateName,
        currentState
      )

  }

  def waitingForemanTask: Receive = {

    // ---------- FROM FOREMAN ---------- //

    case CurrentTask(task) =>
      debugReceive("CurrentTask", sender, "waitingForemanTask")
      // Add the task to the bundle
      this.taskBundle.addTask(task)

      // The broker is busy, the task can return to the worker
      if (this.brokerBusy) {
        if (this.workerBusy) {
          // The worker is also busy, the manager is active
          this.addActiveRecord()
          context become (
            this.active
            orElse this.handleUnexpected("active")
          )
        } else {
          val ownContribution = this.taskBundle.ownerWorkload
          val taskToPerform = this.taskBundle.nextTaskToPerform(
            ownContribution,
            this.contributionMap
          ).get

          // The worker is not busy, the manager give it a task to perform
          this.workerBusy = true
          this.giveToWorker(taskToPerform, "waitingForemanTask")
          this.addActiveRecord()
          context become (
            this.active
            orElse this.handleUnexpected("active")
          )
        }
      }
      // The broker is not busy, the manager tries to delegate a task
      else {
        val (contribution, taskToDelegate) =
          this.delegateOrSplit("waitingForemanTask")

        // There is a task to delegate
        if (taskToDelegate.isDefined) {
          this.brokerBusy = true
          this.giveToBroker(
            taskToDelegate.get,
            contribution,
            "waitingForemanTask"
          )

          // And the worker is busy, the manager is active
          if (this.workerBusy) {
            this.addActiveRecord()
            context become (
              this.active
              orElse this.handleUnexpected("active")
            )
          }
          // The worker is not busy, the manager give it a task to perform
          else {
            val taskToPerform = this.taskBundle.nextTaskToPerform(
              contribution,
              this.contributionMap
            ).get

            this.workerBusy = true
            this.giveToWorker(taskToPerform, "waitingForemanTask")
            this.addActiveRecord()
            context become (
              this.active
              orElse this.handleUnexpected("active")
            )
          }
        }
        // Even the foreman task can not be delegate
        else {
          // The worker is busy, the manager switches in pause state
          if (this.workerBusy) {
            this.addPauseRecord()
            context become (
              this.pause
              orElse this.handleDeprecatedReducerIdleOk("pause")
              orElse this.handleDeprecatedReducerActiveOk("pause")
              orElse this.handleDeprecatedTimeout("pause")
              orElse this.handleUnexpected("pause")
            )
          }
          // The worker is not busy, the manager give it a task to perform
          else {
            val ownContribution = this.taskBundle.ownerWorkload
            val taskToPerform = this.taskBundle.nextTaskToPerform(
              ownContribution,
              this.contributionMap
            ).get

            // As only the worker is busy, the manager switches in pause state
            this.workerBusy = true
            this.giveToWorker(taskToPerform, "waitingForemanTask")
            this.addPauseRecord()
            context become (
              this.pause
              orElse this.handleDeprecatedReducerIdleOk("pause")
              orElse this.handleDeprecatedReducerActiveOk("pause")
              orElse this.handleDeprecatedTimeout("pause")
              orElse this.handleUnexpected("pause")
            )
          }
        }
      }

    case CurrentTaskPerformed =>
      debugReceive("CurrentTaskPerformed", sender, "waitingForemanTask")

      // The broker is busy, the manager is active
      if (this.brokerBusy) {
        this.addActiveRecord()
        context become (
          this.active
          orElse this.handleUnexpected("active")
        )
      } else {
        // The broker is not busy but the worker is, the manager switches in
        // pause state
        if (this.workerBusy) {
          this.addPauseRecord()
          context become (
            this.pause
            orElse this.handleDeprecatedReducerIdleOk("pause")
            orElse this.handleDeprecatedReducerActiveOk("pause")
            orElse this.handleDeprecatedTimeout("pause")
            orElse this.handleUnexpected("pause")
          )
        }
        // Both the broker and the worker are not busy, the manager switches in
        // idle state
        else {
          this.addIdleRecord()
          this goToIdleState "waitingForemanTask"
        }
      }

    // ---------- FROM MONITOR ---------- //

    case ReadyToDie(id) =>
      debugReceive("ReadyToDie", sender, "active")
      debugSend("ReadyToDieAnswer(false)", "active")
      sender ! Monitor.ReadyToDieAnswer(id, ready = false)


  }

  /** Pause state for the manager.
    *
    * It enters this state when no acquaintance seems ready to answer to its
    * CFP.
    *
    * In this state it stores information on acquaintance contribution and
    * leaves this state when if finds a potential bidder for its CFP.
    */
  def pause: Receive = {

    case InformContribution(acquaintance, acqContribution) =>
      debugReceive(
        "InformContribution (" + acquaintance.path.name + " -> " + acqContribution + ")",
        sender,
        "pause"
      )
      this.contributionMap.update(acquaintance, acqContribution)

      // The broker is not busy, the manager looks after a task to delegate
      if (this.taskBundle.nonEmpty && !this.brokerBusy) {
        val (contribution, taskToDelegate) = this.delegateOrSplit("pause")

        // There is a task to delegate, the manager switches in active state
        if (taskToDelegate.isDefined) {
          val task = taskToDelegate.get

          this.brokerBusy = true
          this.addActiveRecord()
          context become (
            this.active
            orElse this.handleUnexpected("active")
          )
          debugSend("Broker.Submit for key " + task.key, "pause")
          this.broker ! Broker.Submit(task, contribution)
        }
      }

    case RequestAndNotBusy(requestedTask: Task) =>
      debugReceive("RequestAndNotBusy", sender, "pause")
      this.taskBundle.addTask(requestedTask)
      this.brokerBusy = false

      // The worker is not busy, the bundle was empty
      // Now that the bundle contains one task, the manager can give it to the
      // worker
      if (!this.workerBusy) {
        val ownContribution = this.taskBundle.ownerWorkload
        val taskToPerform = this.taskBundle.nextTaskToPerform(
          ownContribution,
          this.contributionMap
        ).get

        this.taskBundle.removeTask(taskToPerform)
        this.workerBusy = true
        this.giveToWorker(taskToPerform, "pause")
      }

    case Request(requestedTask) =>
      this.processRequestMessage("pause", requestedTask)

    case WorkerDone(performedCost, nbValues) =>
      debugReceive("WorkerDone", sender, "pause")
      this.workerHasDone()
      this.nbValuesArchive.addRecord(
        self.path.name,
        nbValues
      )
      this.nbValuesPerformed = nbValues :: this.nbValuesPerformed
      this.performedCosts = performedCost :: this.performedCosts

      // Worker has just finished a task then it is useless to require about
      // work in progress
      val ownContribution = this.taskBundle.ownerWorkload

      // Informs all acquaintances about my new contribution
      debugSend("InformContribution(" + ownContribution + ")", "pause")
      context.parent ! InformContribution(self, ownContribution)

      // Was it last task ?
      if (this.taskBundle.isEmpty && this.splitKeys.isEmpty) {
        this.workerBusy = false
        debug("becomes idle")
        this.addIdleRecord()
        this goToIdleState "pause"
      } else {
        // Worker is not busy then manager must give a new task to worker
        // (necessarily one since tasks cannot be empty when in pause)
        val taskToPerform = this.taskBundle.nextTaskToPerform(
          ownContribution,
          this.contributionMap
        ).get

        this.giveToWorker(taskToPerform, "pause")
        this.taskBundle.removeTask(taskToPerform)
      }

    case BrokerNotBusy =>
      debugReceive("BrokerNotBusy", sender, "pause")
      this.brokerBusy = false

    case QueryContribution =>
      debugReceive("QueryContribution", sender, "pause")
      this.brokerBusy = true

      val contribution = this.getContribution

      this.contributionArchive.addRecord(
        self.path.name,
        contribution
      )
      // Informs Broker on task list contribution value
      debugSend("Inform ", "pause")
      sender ! Broker.Inform(contribution)

    // --------- FROM MONITOR ---------- //

    case ReadyToDie(id) =>
      debugReceive("ReadyToDie", sender, "active")
      debugSend("ReadyToDieAnswer(false)", "active")
      sender ! Monitor.ReadyToDieAnswer(id, ready = false)

  }

  /** Idle state of the manager.
    *
    * In this state the manager wait to be potentially reactivated by a
    * negotiation.
    *
    * @param timer current timer
    */
  def idle(timer: ActorRef): Receive = {

    case QueryContribution =>
      debugReceive("QueryContribution", sender, "idle")
      timer ! Timer.Cancel
      this.brokerBusy = true
      debugSend("Inform", "idle")
      sender ! Broker.Inform(0)
      this.addIdleRecord()

    case RequestAndNotBusy(requestedTask) =>
      debugReceive("RequestAndNotBusy", sender, "idle")
      this.brokerBusy = false
      this.workerBusy = true
      this.goToStateWaitingReducerActiveOk("idle", "pause", this.pause)
      this.giveToWorker(requestedTask, "idle")

    case BrokerNotBusy =>
      debugReceive("BrokerNotBusy", sender, "pause")
      this.brokerBusy = false

    case Request(requestedTask) =>
      debugReceive("Request", sender, "idle")
      this.workerBusy = true
      this.goToStateWaitingReducerActiveOk("idle", "pause", this.pause)
      this.giveToWorker(requestedTask, "idle")

    case BrokerReady(_) =>
      debugReceive("BrokerReady", sender, "idle")
      this.brokerBusy = false
      debugSend("Broker.Cancel", "idle")
      sender ! Broker.Cancel

    case InformContribution(initiator, contribution) =>
      debugReceive(
        "InformContribution (" + initiator.path.name + " -> " + contribution + ")",
        sender,
        "idle"
      )
      this.contributionMap.update(initiator, contribution)

    case ReducerIdleOk =>
      debugReceive("ReducerIdleOk", sender, "idle")
      timer ! Timer.Cancel

    case ReadyToDie(id) =>
      debugReceive("ReadyToDie", sender, "idle")
      debugSend("ReadyToDieAnswer(true)", "idle")
      sender ! Monitor.ReadyToDieAnswer(id, ready = true)

    case Timer.Timeout if sender == timer =>
      debugReceive("Timeout", sender, "idle")
      this.addIdleRecord()
      this goToIdleState "idle"

    case AdaptiveReducer.Kill =>
      debugReceive("Kill", sender, "idle")

      val performedValues = this.nbValuesPerformed.sum
      val performedCosts = this.performedCosts.sum
      val managerData = new ManagerData(
        performedValues,
        this.taskBundle match {
          case b: KEligibleTaskBundle => Some(b.getAverageEffectiveK)
          case _                      => None
        },
        this.taskSplitCount,
        this.pauseSwitchCount,
        this.taskBundle.initialTasksByMaxOwnershipRate,
        this.taskBundle.initialTasksByOwnerOwnershipRate
      )

      managerData.write(this.config.resultPath + self.path.name + ".csv")
      this.contributionArchive.writeCSV(
        "ID;Contribution",
        { case (id, contrib) => id + ";" + contrib }
      )
      this.nbValuesArchive.writeCSV(
        "ID;Performed cost",
        { case (id, cost) => id + ";" + cost }
      )
      this.pauseArchive.writeCSV(
        "ID;In pause state?",
        { case (id, pauseState) => id + ";" + pauseState }
      )
      this.idleArchive.writeCSV(
        "ID;In idle state?",
        { case (id, idleState) => id + ";" + idleState }
      )
      this.taskSplitArchive.writeCSV("Number of task split", _.toString)

      val nbValuesFile = new File(
        this.config.resultPath + context.parent.path.name + "_nbValues.dat"
      )
      val costsFile = new File(
        this.config.resultPath + context.parent.path.name + "_costs.dat"
      )

      for (
        _ <- 0 to this.config.gnuplotMaxTaskDoneNumber - this.nbValuesPerformed.length
      ) {
        this.nbValuesPerformed = 0 :: this.nbValuesPerformed
        this.performedCosts = 0 :: this.performedCosts
      }

      // Write file with performed values
      FileWriter.writeWith(
        nbValuesFile,
        Iterator(
          context.parent.path.name.substring(
            Monitor.reducerNamePrefix.length) +
            " " +
            (performedValues :: this.nbValuesPerformed.reverse).
              addString(new StringBuilder(), " ") + "\n"
        )
      )
      // Write file with performed costs
      FileWriter.writeWith(
        costsFile,
        Iterator(
          context.parent.path.name.substring(
            Monitor.reducerNamePrefix.length) +
            " " +
            (performedCosts :: this.performedCosts.reverse).
              addString(new StringBuilder(), " ") + "\n"
        )
      )
      debugSend("KillOk", "idle")
      sender ! AdaptiveReducer.ManagerKillOk(managerData)
      context stop self

  }

  /** Handle deprecated ReducerIdleOk messages.
    *
    * @param state state in which the deprecated ReducerIdleOk message is
    *              received
    */
  def handleDeprecatedReducerIdleOk(state: String): Receive = {

    case ReducerIdleOk =>
      debugDeprecated("ReducerIdleOk", sender, state)

  }

  /** Handle deprecated ReducerActiveOk messages.
    *
    * @param state state in which the deprecated ReducerActiveOk message is
    *              received
    */
  def handleDeprecatedReducerActiveOk(state: String): Receive = {

    case ReducerActiveOk =>
      debugDeprecated("ReducerActiveOk", sender, state)

  }

  /** Handle deprecated Timeout messages.
    *
    * @param state state in which the deprecated Timeout message is received
    */
  def handleDeprecatedTimeout(state: String): Receive = {

    case Timer.Timeout =>
      debugDeprecated("Timeout", sender, state)

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_         =>
      debugUnexpected(self, sender, state, msg)

  }

}
