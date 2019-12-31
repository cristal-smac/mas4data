package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Stash}
import akka.util.Timeout
import akka.pattern.ask
import utils.config.ConfigurationBuilder
import utils.tasks.{IrTask, Task}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

/** Companion object of the Foreman agent.
  *
  * Contain all the messages that a foreman could receive.
  */
object Foreman {

  // ---------- FROM REDUCER ---------- //

  /** Go message
    *
    * Inform the foreman that it can start to work.
    */
  object Go

  // ---------- FROM MANAGER ---------- //

  /** Perform message
    *
    * Request from the manager to perform the task.
    */
  case class Perform(task: Task)

  /** GetCurrentTask message
    *
    * Request from the manager to get what it remains from the currently
    * performed task.
    */
  object GetCurrentTask

  /** QueryRemainingWork message
    *
    * Request from the manager to know the remaining work.
    */
  object QueryRemainingWork

  // ---------- FROM WORKER ---------- //

  /** WorkerDone message
    *
    * Inform the foreman that the worker has finished a piece of the current
    * task.
    */
  object WorkerDone

  /** Remaining message
    *
    * Inform the foreman of the current worker remaining work.
    *
    * @param remains current remaining work
    */
  case class Remaining(remains: Long)

}

/** Represent the foreman of an adaptive reducer.
  *
  * A foreman is a middleman between a manager and a worker. It receives the
  * current task to perform from the manager and give it piece by piece to the
  * worker.
  */
class Foreman(
  worker: ActorRef,
  rfh: ActorRef
) extends Actor with Stash with utils.debugs.Debug {

  import Foreman._

  this.setDebug(ConfigurationBuilder.config.debugs("debug-worker"))

  // Contains the number of value of the current task which is already performed
  // by the worker
  private var nbPerformedValues: Long = 0

  // Contains the current task cost which is already performed by the worker
  private var performedCost: Long = 0

  private val performedOwnership: mutable.Map[Double, Int] =
    mutable.Map[Double, Int]()

  // Reset the performed costs variables
  private def resetPerformedCosts(): Unit = {
    this.nbPerformedValues = 0
    this.performedCost = 0
  }

  // Update stats about a performed task
  private def addPerformedStats(task: Task): Unit = {
    val ownershipRate = task.ownershipRate(this.rfh)

    if (this.performedOwnership.contains(ownershipRate)) {
      this.performedOwnership.update(
        ownershipRate,
        this.performedOwnership(ownershipRate) + 1
      )
    } else {
      this.performedOwnership.put(ownershipRate, 1)
    }

    this.nbPerformedValues += task.nbValues
    this.performedCost += task.cost(this.rfh)
  }

  // Extract a chunk from the current task in order to give it to the worker
  private def giveAChunk(task: Task): (Option[Task], Task) = task match {
    // The current task is already an IR task
    case irt: IrTask =>
      val newCurrentTask = if (!irt.canBeDivided) None else {
        Some(
          new IrTask(
            irt.key,
            irt.chunks.tail,
            irt.frCost,
            irt.finalPerformer
          )
        )
      }
      val taskToPerform = new IrTask(
        irt.key,
        List(irt.chunks.head),
        irt.frCost,
        irt.finalPerformer
      )

      (newCurrentTask, taskToPerform)

    // The current task is an FR task
    case _           => if (!task.canBeDivided) (None, task) else {
      val newCurrentTask = Some(
        new IrTask(
          task.key,
          task.chunks.tail,
          task.nbValues,
          context.parent
        )
      )
      val taskToPerform =
        new IrTask(
          task.key,
          List(task.chunks.head),
          task.nbValues,
          context.parent
        )

      (newCurrentTask, taskToPerform)
    }
  }

  // Get the remaining work from the worker
  private def getRemainingWork(currentRemaining: Long): Long = {
    implicit val timeout: Timeout = Timeout(Duration(1000, "millis"))

    debugSend("GetRemainingWork[synchronous] to worker", "active")

    val future = this.worker ? Worker.GetRemainingWork
    val taskInProgressCost = {
      val remainingMsg = try {
        Await.result(future, timeout.duration).asInstanceOf[Remaining]
      } catch {
        case _: Exception => Remaining(0)
      }

      remainingMsg.remains
    }

    currentRemaining + taskInProgressCost
  }

  debugSend("AdaptiveReducer.Ready", "building...")
  context.parent ! AdaptiveReducer.Ready

  def receive: Receive = this.waitGo orElse this.stashAll

  /** State in which the foreman stash all received messages in order to handle
    * them later.
    */
  def stashAll: Receive = {

    case msg@_ =>
      debug("Foreman has to stash message " + msg)
      stash()

  }

  /** State in which the foreman waits for the green light from the reducer. */
  def waitGo: Receive = {

    case Go =>
      debugReceive("Go from the reducer", sender, "waitGo")
      unstashAll()
      context become (
        this.waiting
        orElse this.handleUnexpected("waiting")
      )

  }

  /** State in which the foreman waits the next task to perform from the
    * manager.
    */
  def waiting: Receive = {

    case Perform(task) =>
      debugReceive("Perform from the manager", sender, "waiting")

      val (currentTask, taskToPerform) = this giveAChunk task

      this.resetPerformedCosts()

      // Send the task to perform to the worker
      debugSend("Worker.Perform", "waiting")
      this.worker ! Worker.Perform(taskToPerform)
      this.addPerformedStats(taskToPerform)
      // Switch in active state
      context become (
        this.active(currentTask, sender)
        orElse this.handleUnexpected("active")
      )

    case AdaptiveReducer.Kill =>
      debugReceive("AdaptiveReduce.Kill", sender, "free")
      debugSend("AdaptiveReducer.ForemanKillOk", "waiting")
      sender ! AdaptiveReducer.ForemanKillOk(this.performedOwnership)
      context stop self

    case QueryRemainingWork =>
      debugReceive("QueryRemainingWork", sender, "waiting")
      sender ! Manager.Remaining(0)

  }

  /** State in which the foreman handles a task with the worker. */
  def active(currentTask: Option[Task], manager: ActorRef): Receive = {

    // ---------- FROM WORKER ---------- //

    // The current task is not totally performed
    case WorkerDone if currentTask.isDefined =>
      debugReceive("WorkerDone", sender, "active")

      val (newCurrentTask, taskToPerform) = this giveAChunk currentTask.get

      // Send the task to perform to the worker
      debugSend("Worker.Perform", "waiting")
      this.worker ! Worker.Perform(taskToPerform)
      this.addPerformedStats(taskToPerform)
      // Switch in active state
      context become (
        this.active(newCurrentTask, manager)
        orElse this.handleUnexpected("active")
      )

    // The current task is totally performed
    case WorkerDone =>
      debugReceive("WorkerDone", sender, "active")
      // Follow the message to the manager
      debugSend("Manager.WorkerDone", "active")
      manager ! Manager.WorkerDone(this.performedCost, this.nbPerformedValues)
      context become (this.waiting orElse this.handleUnexpected("waiting"))

    // ---------- FROM MANAGER ---------- //

    case QueryRemainingWork =>
      debugReceive("QueryRemainingWork", sender, "active")

      val remainingCost = if (currentTask.isDefined) {
        this.getRemainingWork(currentTask.get.nbValues)
      } else {
        this.getRemainingWork(0)
      }

      sender ! Manager.Remaining(remainingCost)

    // The current task is not totally performed
    case GetCurrentTask if currentTask.isDefined =>
      debugReceive("GetCurrentTask", sender, "active")
      // Send the current task to the manager
      manager ! Manager.CurrentTask(currentTask.get)
      context become (
        this.active(None, manager)
        orElse this.handleUnexpected("active")
      )

    // The current task is totally performed
    case GetCurrentTask =>
      debugReceive("GetCurrentTask", sender, "active")
      // Inform the manager that the current task is totally performed
      manager ! Manager.CurrentTaskPerformed

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}
