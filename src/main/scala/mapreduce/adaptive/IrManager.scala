package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Stash}

import scala.collection.mutable
import utils.config.ConfigurationBuilder
import utils.jobs.{MapResult, ReduceJob}
import utils.tasks.{Chunk, IrResult, MemorySource, TaskBuilder}

/** Companion object of the IrManager class.
  *
  * Contain all the messages that an IR manager could receive.
  */
object IrManager {

  // ----- REDUCER ----- //

  /** Message Go
    *
    * Inform the IR manager that it can start to work.
    */
  object Go

  // ----- WORKER ----- //

  /** Message GetIrResult
    *
    * Give an IR result to the IR manager.
    */
  case class GetIrResult(irResult: IrResult)

}

/** Represent the IR manager of a reducer.
  *
  * The IR manager handles the IR result bundle, i.e. aggregates IR results,
  * sends FR tasks to the manager.
  *
  * @param irJob   job to apply on the IR results
  * @param manager manager to communicate with
  * @param rfh     remote file handler which deals with this foreman
  */
class IrManager(
  irJob: ReduceJob,
  manager: ActorRef,
  rfh: ActorRef
) extends Actor with Stash with utils.debugs.Debug {

  import IrManager._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-manager"))

  debugSend("AdaptiveReducer.Ready", "building...")
  context.parent ! AdaptiveReducer.Ready

  // Contains all the IR results waiting to be merged in a complete task
  private val irResultBundle = mutable.Map.empty[String, IrResult]

  def receive: Receive = this.waitGo orElse this.stashAll

  /** State in which the IR manager stash all the received messages in order to
    * handle them later.
    */
  def stashAll: Receive = {

    case msg@_ =>
      debug("!!! IR manager has to stash message " + msg + " !!!")
      stash()

  }

  /** State in which the IR manager waits the green light of the reducer. */
  def waitGo: Receive = {

    case Go =>
      debugReceive("Go from the reducer", sender, "waitGo")
      unstashAll()
      context become (
        this.active
        orElse this.handleUnexpected("active")
      )

  }

  /** State in which the IR manager is active. */
  def active: Receive = {

    case GetIrResult(irResult) =>
      debugReceive("GetIrResult for key: " + irResult.key, sender, "active")

      if (this.irResultBundle contains irResult.key) {
        val otherIrResult = this.irResultBundle(irResult.key)
        val mergedIrResult = irResult mergeWith otherIrResult

        if (mergedIrResult.canBeTransformedInFrTask) {
          val recoveredMapKey = this.irJob recoverMapKey mergedIrResult.key
          val recoveredMapValues =
            mergedIrResult.values map this.irJob.recoverMapValue
          val recoveredMapResult =
            MapResult(recoveredMapKey, recoveredMapValues)
          val dataSource =
            MemorySource.createMemorySource(recoveredMapResult.toIterator)
          val chunk = new Chunk(dataSource, 0, this.rfh)
          val task = TaskBuilder.buildTask(mergedIrResult.key, List(chunk))

          debug(self.path.name + ": MERGE TASK for key " + task.key)
          this.manager ! Manager.Request(task)
          this.irResultBundle -= irResult.key
        } else {
          this.irResultBundle.update(irResult.key, mergedIrResult)
        }
      } else {
        this.irResultBundle += (irResult.key -> irResult)
      }

    case AdaptiveReducer.Kill =>
      debugReceive("Kill", sender, "active")
      sender ! AdaptiveReducer.IrManagerKillOk
      context stop self

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}
