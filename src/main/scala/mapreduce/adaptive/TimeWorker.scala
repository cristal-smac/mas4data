package mapreduce.adaptive

import java.io.File

import akka.actor.{Actor, ActorRef, Props, Stash}
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.experiments.WorkerData
import utils.files.FileWriter
import utils.tasks.{IrResult, IrTask, Task}

import scala.collection.mutable

/** Worker which does not really perform tasks but fake it with a timer.
  *
  * @param timeFunction function which give a time to wait regarding a task
  * @param rfhMap       map which associates a reducer to its RFH
  */
class TimeWorker(
  timeFunction: Task => Int,
  rfhMap: Map[ActorRef, ActorRef]
) extends Actor with Stash with utils.debugs.Debug {

  import Worker._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-worker"))

  // Working time by key of the worker
  private val workingTimeByKey = mutable.Map.empty[String, Long]

  // Total working time of the worker
  private var totalWorkingTime: Long = 0

  // Update the working time map
  private def updateWorkingTime(key: String, time: Long): Unit = {
    // Update the total working time
    this.totalWorkingTime += time

    // Update the working time by key map
    if (this.workingTimeByKey contains key) {
      this.workingTimeByKey.update(key, this.workingTimeByKey(key) + time)
    } else {
      this.workingTimeByKey += (key -> time)
    }
  }

  debugSend("AdaptiveReducer.Ready", "building...")
  context.parent ! AdaptiveReducer.Ready

  def receive: Receive = this.waitGo orElse this.stashAll

  /** State in which the worker waits the green light of the reducer. */
  def waitGo: Receive = {

    case Go =>
      unstashAll()
      context become (
        this.free
        orElse this.handleUnexpected("free")
      )

  }

  /** State in which the worker stash all the received messages in order to
    * handle them later.
    */
  def stashAll: Receive = {

    case msg@_ =>
      debug("!!! worker has to stash message " + msg + " !!!")
      stash()

  }

  /** State in which the worker waits a task to perform. */
  def free: Receive = {

    case Perform(task) =>
      debugReceive("Perform", sender, "free")

      val taskDuration = this.timeFunction(task)
      val timer = context.actorOf(Props(classOf[Timer], taskDuration))

      timer ! Timer.Start
      context become (
        this.busy(task, taskDuration, sender, timer)
        orElse this.handleUnexpected("busy")
      )

    case AdaptiveReducer.Kill =>
      debugReceive("Kill", sender, "free")

      val workerData = new WorkerData(
        totalPerformedChunks        = 0,
        totalLocallyPerformedChunks = 0,
        totalWorkingTime            = this.totalWorkingTime.toDouble / 1e6,
        totalWaitingTime            = 0,
        totalFreeTime               = 0,
        totalBusyTime               = 0
      )

      debugSend("KillOk", "free")
      sender ! AdaptiveReducer.WorkerKillOk(workerData)
      this.die(workerData)

  }

  /** State in which the worker is performing a task.
    *
    * @param task         performing task
    * @param taskDuration duration of the task
    * @param foreman      foreman of the worker
    * @param timer        current timer
    */
  def busy(
    task: Task,
    taskDuration: Int,
    foreman: ActorRef,
    timer: ActorRef
  ): Receive = {

    case Timer.Timeout if sender == timer =>
      task match {
        case irt: IrTask =>
          val finalPerformer = irt.finalPerformer

          finalPerformer ! IrManager.GetIrResult(
            IrResult(
              irt.key.toString,
              List("1"),
              irt.nbValues,
              irt.frCost,
              finalPerformer
            )
          )

        case _ =>
      }
      debugReceive("Timer.Timeout", sender, "busy")
      debugSend("Foreman.WorkerDone", "busy")
      foreman ! Foreman.WorkerDone
      this.updateWorkingTime(task.key, taskDuration)
      context become (
        this.free
        orElse this.handleUnexpected("free")
      )

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

  /** Kill process of the worker. */
  private def die(workerData: WorkerData): Unit = {
    val workingFile = new File(
      this.config.resultPath + "details_" + self.path.name + ".csv"
    )
    val workingTimeInMs = this.workingTimeByKey map {
      case (key, nanoTime) => (key, nanoTime / 1e6)
    }

    workerData.write(this.config.resultPath + self.path.name + ".csv")
    FileWriter.writeWith(
      workingFile,
      Iterator(
        "Total working time (ms): " + (this.totalWorkingTime / 1e6),
        self.path.name + ": (key -> working time in ms)",
        workingTimeInMs mkString "\n"
      )
    )
    context stop self
  }

}
