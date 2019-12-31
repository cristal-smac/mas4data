package mapreduce.adaptive

import java.io.File
import java.lang.management.ManagementFactory

import akka.actor.{Actor, ActorRef, Stash}
import mapreduce.filesystem.RemoteFileHandler
import utils.config.ConfigurationBuilder
import utils.experiments.{Archive, WorkerData}
import utils.files.FileWriter
import utils.jobs.{ReduceJob, ReduceResult}
import utils.tasks._

import scala.collection.mutable
import scala.language.postfixOps

/** Companion object of the Worker class.
  *
  * Contains all the messages that a worker could receive.
  */
object Worker {

  // ---------- FROM REDUCER ---------- //

  /** Message Go
    *
    * Inform the worker that it can begin to work.
    */
  object Go

  // ---------- FROM FOREMAN ---------- //

  /** Message Perform
    *
    * Ask to the worker to perform its job on a given task.
    *
    * @param task task to perform
    */
  case class Perform(task: Task)

  /** Message GetRemainingWork
    *
    * Ask the current remaining work to the worker.
    */
  case object GetRemainingWork

  // ---------- FROM WORKER ---------- //

  /** Message TaskFinished
    *
    * Inform the worker that its task is finished.
    */
  object TaskFinished

  /** Message IrTaskFinished
    *
    * Inform the worker that its IR task is finished.
    *
    * @param result         result of the finished IR task
    * @param irCost         cost of the result IR task
    * @param frCost         cost of the initial FR task
    * @param finalPerformer final performer of the IR task
    */
  case class IrTaskFinished[K, V](
    result: ReduceResult[K, V],
    irCost: Long,
    frCost: Long,
    finalPerformer: ActorRef
  )

}

/** Represent the worker of an adaptive reducer.
  *
  * A worker executes the tasks of a reducer in a map reduce system.
  *
  * @param reduceJob job to apply on the task
  * @param rfh       remote file handler which deals with this worker
  */
class Worker(
  val reduceJob: ReduceJob,
  val rfh: ActorRef
) extends Actor with Stash with utils.debugs.Debug {

  import Worker._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-worker"))

  // Aggregation of the results product by the worker.
  private var results: List[ReduceResult[this.reduceJob.K, this.reduceJob.V]] =
    Nil

  // Current task cost
  private var currentTaskCost: Long = 0

  // The runnable task run by this reducer.
  private var runnableTask: RunnableTask = _

  // To measure the waiting RFH time
  private var t: Long = 0

  // Total waiting time of the worker
  private var totalWaitingTime: Long = 0

  // To measure the time the worker is in the busy state
  private var tBusy: Long = 0

  // Total busy time of the worker
  private var totalBusyTime: Long = 0

  // To measure the time the worker is in the free state
  private var tFree: Long = 0

  // Total free time of the worker
  private var totalFreeTime: Long = 0

  // Count the number of chunks which has been locally performed
  private var locallyPerformedChunks: Int = 0

  // Count the number of chunks which has been performed
  private var performedChunks: Int = 0

  var killThread: Boolean = false

  // Working time by key of the worker
  private val workingTimeByKey = mutable.Map.empty[String, Long]

  // Total working time of the worker
  private var totalWorkingTime: Long = 0

  // Archive to record when the worker is in free state or not
  private val freeArchive: Archive[(String, Boolean), Int] =
    new Archive[(String, Boolean), Int](self.path.name + "_free")

  // Add a record to specify that the worker is free
  private def addFreeRecord(): Unit =
    this.freeArchive.addRecord(self.path.name, true)

  // Add a record to specify that the worker is busy
  private def addBusyRecord(): Unit =
    this.freeArchive.addRecord(self.path.name, false)

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

  // Get the CPU time of a given thread
  private def getCpuTime(thread: Thread): Long = {
    val mxBean = ManagementFactory.getThreadMXBean

    if (mxBean.isThreadCpuTimeSupported) {
      try {
        mxBean.getThreadCpuTime(thread.getId)
      } catch {
        case _: UnsupportedOperationException =>
          0
      }
    } else {
      0
    }
  }

  debugSend("AdaptiveReducer.Ready", "building...")
  context.parent ! AdaptiveReducer.Ready

  def receive: Receive = this.waitGo orElse this.stashAll

  /** State in which the worker waits the green light of the reducer. */
  def waitGo: Receive = {

    case Go =>
      unstashAll()
      this.tFree = System.currentTimeMillis()
      this.addFreeRecord()
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
      this.addBusyRecord()
      context become (
        this.waitingFiles(task, sender)
        orElse this.handleUnexpected("waitingFiles")
      )
      // Count the number of local chunks
      task.chunks foreach {
        chunk =>
          if (chunk.owner == this.rfh) this.locallyPerformedChunks += 1
          this.performedChunks += 1
      }
      debugSend("RemoteFileHandler.GetDataFor", "free")
      this.rfh ! RemoteFileHandler.GetDataFor(task.chunks)
      this.t = System.currentTimeMillis()
      this.totalFreeTime += this.t - this.tFree
      this.currentTaskCost = task.nbValues

    case GetRemainingWork =>
      debugReceive("GetRemainingWork", sender, "free")
      debugSend("Foreman.Remaining", "free")
      sender ! Foreman.Remaining(0)

    case AdaptiveReducer.Kill =>
      debugReceive("Kill", sender, "free")
      this.totalFreeTime += System.currentTimeMillis() - this.tFree

      val workerData = new WorkerData(
        totalPerformedChunks        = this.performedChunks,
        totalLocallyPerformedChunks = this.locallyPerformedChunks,
        totalWorkingTime            = this.totalWorkingTime.toDouble / 1e6,
        totalWaitingTime            = this.totalWaitingTime,
        totalFreeTime               = this.totalFreeTime,
        totalBusyTime               = this.totalBusyTime
      )

      this.freeArchive.writeCSV(
        "ID;In free state?",
        { case (id, freeState) => id + ";" + freeState }
      )
      this.die(workerData, sender)

  }

  /** State in which the worker waits that all the files are in its node in
    * order to perform a task.
    */
  def waitingFiles(task: Task, foreman: ActorRef): Receive = {

    case RemoteFileHandler.AvailableDataFor(chunks) =>
      debugReceive("RemoteFileHandler.AvailableDataFor " + task, sender, "waitingFiles")
      this.totalWaitingTime += System.currentTimeMillis() - this.t
      context become (
        this.launching(foreman, task.nbValues)
        orElse this.handleUnexpected("launching")
      )
      debugSend("Perform to self", "waitingFiles")
      self ! Perform(TaskBuilder.buildTask(task.key, chunks))

    case GetRemainingWork =>
      debugReceive("GetRemainingWork", sender, "waitingFiles")

      val remainingCost = task.cost(this.rfh)

      debugSend("Remaining(" + remainingCost + ")", "waitingFiles")
      sender ! Foreman.Remaining(remainingCost)

  }

  /** State in which the worker starts to perform a task. */
  def launching(foreman: ActorRef, costToPerform: Long): Receive = {

    case Perform(task) if sender == self => task match {
      case irt: IrTask =>
        debugReceive(
          "Perform for a task of size " + this.currentTaskCost,
          sender,
          "launching"
        )
        // builds runnable task
        this.runnableTask = new IrRunnableTask(
          irt,
          self,
          irt.nbValues,
          irt.frCost,
          irt.finalPerformer
        )
        this.killThread = false

        // start task thread
        val thread = new Thread(this.runnableTask)

        debug("runnable thread started")
        thread.start()
        this.tBusy = System.currentTimeMillis()
        context become (
          this.busy(foreman, thread, task)
          orElse this.handleUnexpected("busy")
        )

      case t: Task =>
        debugReceive(
          "Perform for a task of size " + this.currentTaskCost,
          sender,
          "launching"
        )
        // builds runnable task
        this.runnableTask = new FrRunnableTask(t, self)
        this.killThread = false

        // start task thread
        val thread = new Thread(this.runnableTask)

        debug("runnable thread started")
        thread.start()
        this.tBusy = System.currentTimeMillis()
        context become (
          this.busy(foreman, thread, task)
          orElse this.handleUnexpected("busy")
        )
    }

    case GetRemainingWork =>
      debugReceive("GetRemainingWork", sender, "launching")
      debugSend("Remaining(" + costToPerform + ")", "launching")
      sender ! Foreman.Remaining(costToPerform)

  }

  /** State in which the worker is performing a task.
    *
    * @param foreman       foreman of the worker
    * @param currentThread thread in which the current task is performed
    * @param currentTask   performed task
    */
  def busy(
    foreman: ActorRef,
    currentThread: Thread,
    currentTask: Task
  ): Receive = {

    case TaskFinished =>
      debug("task is finished")

      val cpuTime = this.getCpuTime(currentThread)

      // Allow the current thread to die
      this.killThread = true
      this.synchronized {
        notifyAll()
      }
      this.totalBusyTime += System.currentTimeMillis() - this.tBusy
      // Informs foreman that task is completed
      debugSend("WorkerDone", "busy")
      foreman ! Foreman.WorkerDone
      this.updateWorkingTime(currentTask.key, cpuTime)
      this.tFree = System.currentTimeMillis()
      this.addFreeRecord()
      context become (
        this.free
        orElse this.handleUnexpected("free")
      )

    case IrTaskFinished(result, irCost, frCost, finalPerformer) =>
      debug("IR task is finished for key " + result.key)

      val cpuTime = this.getCpuTime(currentThread)

      // Allow the current thread to die
      this.killThread = true
      this.synchronized {
        notifyAll()
      }
      this.totalBusyTime += System.currentTimeMillis() - this.tBusy
      // Send the IR result to the final performer
      finalPerformer ! IrManager.GetIrResult(
        IrResult(
          result.key.toString,
          List(result.value.toString),
          irCost,
          frCost,
          finalPerformer
        )
      )
      debugSend("WorkerDone", "busy")
      foreman ! Foreman.WorkerDone
      this.updateWorkingTime(currentTask.key, cpuTime)
      this.tFree = System.currentTimeMillis()
      this.addFreeRecord()
      context become (
        this.free
        orElse this.handleUnexpected("free")
      )

    case GetRemainingWork =>
      debugReceive("GetRemainingWork", sender, "busy")

      val remainingWork = currentTask.nbValues - this.reduceJob.getCounter

      debugSend("Foreman.Remaining(" + remainingWork + ")", "busy")
      sender ! Foreman.Remaining(remainingWork)

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

  /** Kill process of the worker. */
  private def die(workerData: WorkerData, reducer: ActorRef): Unit = {
    val resultFile = new File(this.config.resultPath + self.path.name + ".dat")
    val workingFile = new File(
      this.config.resultPath + "details_" + self.path.name + ".csv"
    )
    val workingTimeInMs = this.workingTimeByKey map {
      case (key, nanoTime) => (key, nanoTime / 1e6)
    }

    workerData.write(this.config.resultPath + self.path.name + ".csv")
    FileWriter.writeWith(
      resultFile,
      this.reduceJob transformResults this.results
    )
    FileWriter.writeWith(
      workingFile,
      Iterator(
        "Total working time (ms): " + (this.totalWorkingTime / 1e6),
        self.path.name + ": (key -> working time in ms)",
        workingTimeInMs mkString "\n"
      )
    )
    debugSend("KillOk", "free")
    reducer ! AdaptiveReducer.WorkerKillOk(workerData)
    context stop self
  }

  /** Trait for a task which is runnable in a independent thread. */
  private trait RunnableTask extends Runnable

  /** Thread to perform a FR task.
    *
    * @param task   the FR task
    * @param sender the actor that ask for the data to be reduced and which
    *               must be inform when task is done
    */
  private class FrRunnableTask(
    task: Task,
    sender: ActorRef
  ) extends RunnableTask {

    def run(): Unit = {
      val result = Worker.this.reduceJob reduce this.task

      Worker.this.results = result :: Worker.this.results
      this.sender ! TaskFinished

      Worker.this.synchronized {
        Worker.this.wait()
      }
    }

  }

  /** Thread to perform an IR task, task and data are provided at construction
    *
    * @param task           the IR task
    * @param sender         the actor that ask for the data to be reduced and
    *                       which must be inform when task is done
    * @param irCost         cost of the IR task
    * @param frCost         cost of the FR task from which comes the IR task
    * @param finalPerformer final performer of the IR task
    */
  private class IrRunnableTask(
    task: Task,
    sender: ActorRef,
    irCost: Long,
    frCost: Long,
    finalPerformer: ActorRef
  ) extends RunnableTask {

    def run(): Unit = {
      val result = Worker.this.reduceJob intermediateReduce this.task

      this.sender ! IrTaskFinished(
        result,
        this.irCost,
        this.frCost,
        this.finalPerformer
      )

      Worker.this.synchronized {
        Worker.this.wait()
      }
    }

  }

}
