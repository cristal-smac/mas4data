package mapreduce.classic

import java.io.File

import akka.actor.{Actor, ActorRef, Props}
import mapreduce.filesystem.RemoteFileHandler
import mapreduce.time.Timer
import utils.config.{Configuration, ConfigurationBuilder}
import utils.files.FileWriter
import utils.jobs.{ReduceJob, ReduceResult}
import utils.tasks._


/** Companion object of the Reducer class.
  *
  * Contain all the messages that a reducer could receive.
  */
object Reducer {

  /** Message WorkOn
    *
    * Ask to the reducer to work on given tasks.
    *
    * @param tasks tasks to work on
    */
  case class WorkOn(tasks: List[Task])

  /** Message Perform
    *
    * Ask to the worker to perform its job on a given task.
    *
    * @param task task on which execute the reduce job
    */
  case class Perform(task: Task)

  /** Message Execute
    *
    * Ask to the reducer to execute its job on the tasks it received by the
    * mappers.
    */
  case object Execute

  /** Message ReducerDoneOk
    *
    * Acknowledgment for the ReducerDone message.
    */
  case object ReducerDoneOk

  /** Message Kill
    *
    * Ask to the reducer to kill itself.
    */
  case object Kill

  /** Message Abort
    *
    * Ask to the reducer to abort.
    */
  case object Abort

}

/** Represent a reducer in a classic map reduce system.
  *
  * @constructor create a new reducer agent
  * @param job reduce job to execute
  */
class Reducer(
  val job: ReduceJob,
  rfh: ActorRef
) extends Actor with utils.debugs.Debug {

  import Reducer._

  // Worker of the reducer
  val worker: ActorRef =
    context.actorOf(Props(classOf[Worker], this), name = "worker")

  // Configuration object
  val config: Configuration = ConfigurationBuilder.config

  // Total cost processed by the reducer
  private var totalCost: Long = 0

  // List of costs of performed tasks
  private var costs: List[Long] = List()

  private var results: List[ReduceResult[this.job.K, this.job.V]] = Nil

  // Pass in the waitSupervisor state
  private def goToWaitSupervisorState(): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitSupervisor(timer)
      orElse this.handleUnexpected("waitSupervisor")
    )
    context.parent ! Supervisor.ReducerDone
    timer ! Timer.Start
  }

  def receive: Receive =
    this.waitTasks(Map[String, Task](), List[ActorRef]()) orElse
    this.handleDeprecatedWorkOn("waitTasks") orElse
    this.handleUnexpected("waitTasks")

  /** State in which the reducer waits the mappers deliver their results.
    *
    * @param tasks           current tasks
    * @param alreadyReceived set which contains tasks the reducer already
    *                        received
    */
  def waitTasks(
    tasks: Map[String, Task],
    alreadyReceived: List[ActorRef]
  ): Receive = {

    case WorkOn(receivedTasks) if !(alreadyReceived contains sender) =>
      debugReceive("WorkOn", sender, "waitTasks")
      debugSend("AdaptiveMapper.WorkOnOk", "waitTasks")
      sender ! Mapper.WorkOnOK

      val newTasks = receivedTasks.foldLeft(tasks) {
        case (innerTasks, task) =>
          val key = task.key

          if (innerTasks contains key) {
            innerTasks.updated(
              key,
              TaskBuilder.mergeTasks(innerTasks(key), task)
            )
          } else {
            innerTasks + (key -> task)
          }
      }

      context become (
        this.waitTasks(newTasks, sender :: alreadyReceived)
        orElse this.handleDeprecatedWorkOn("waitTasks")
        orElse this.handleUnexpected("waitTasks")
      )

    case msg@Execute =>
      // Build the valued tasks
      val tasksToExecute = tasks.values.toList

      context become (
        this.execution(tasksToExecute)
        orElse this.handleUnexpected("execution")
      )
      self ! msg

  }

  /** State in which the reducer executes its job.
    *
    * @param tasks tasks on which apply the reduce job
    */
  def execution(tasks: List[Task]): Receive = {

    case Execute if sender == self && tasks.nonEmpty =>
      val task = tasks.head

      this.totalCost += task.nbValues
      this.costs = task.nbValues :: this.costs
      context become (
        this execution tasks.tail
        orElse handleUnexpected("execution")
      )
      this.worker ! Perform(task)

    case Execute if sender == self && tasks.isEmpty =>
      val file = new File(this.config.resultPath + self.path.name)

      FileWriter.writeWith(file, this.job transformResults this.results)
      this.goToWaitSupervisorState()

    case Worker.WorkDone(result) if tasks.nonEmpty =>
      val task = tasks.head

      this.results = result :: this.results
      this.totalCost += task.nbValues
      this.costs = task.nbValues :: this.costs
      context become (
        this execution tasks.tail
        orElse handleUnexpected("execution")
      )
      this.worker ! Perform(task)

    case Worker.WorkDone(result) if tasks.isEmpty =>
      this.results = result :: this.results

      val file = new File(this.config.resultPath + "worker@" + self.path.name)

      FileWriter.writeWith(file, this.job transformResults this.results)
      this.goToWaitSupervisorState()

  }

  /** State in which the reducer waits for the supervisor.
    *
    * @param timer current timer
    */
  def waitSupervisor(timer: ActorRef): Receive = {

    case ReducerDoneOk =>
      timer ! Timer.Cancel
      context become (
        this.waitProperKill
        orElse this.handleDeprecatedTimeout("waitProperKill")
        orElse this.handleUnexpected("waitProperKill")
      )

    case Timer.Timeout if sender == timer => this.goToWaitSupervisorState()

  }

  /** State in which the reducer wait for the supervisor green light to kill
    * itself.
    */
  def waitProperKill: Receive = {

    case Kill =>
      context.parent ! Supervisor.KillOk
      context stop self

  }

  /** Handle the deprecated WorkOn messages.
    *
    * @param state state in which the deprecated WorkOn message is received
    */
  def handleDeprecatedWorkOn(state: String): Receive = {

    case WorkOn(_) =>
      debugDeprecated("WorkOn", sender, state)
      sender ! Mapper.WorkOnOK

  }

  /** Handle the deprecated Timeout messages.
    *
    * @param state state in which the deprecated Timeout message is received
    */
  def handleDeprecatedTimeout(state: String): Receive = {

    case Timer.Timeout => debugDeprecated("Timeout", sender, state)

  }

  /** Handle the unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

  /** Write the output. */
  override def postStop: Unit = {
    val file = new File(this.config.resultPath + self.path.name + ".csv")
    var costs = this.costs

    for (_ <- 0 to this.config.gnuplotMaxTaskDoneNumber - costs.length) {
      costs = 0 :: costs
    }

    FileWriter.writeWith(
      file,
      Iterator(
        //context.parent.path.name.substring(7) + "\t" + this.totalCost.toString
          self.path.name.substring(7) + " " + costs.reverse.addString(new StringBuilder()," ")
        ))
  }

  /** Companion object of the classic worker agent. */
  object Worker {

    /** Message WorkerDone: inform the reducer that the worker has finished its
    * job.
    *
    * @param result result of the worker job
    */
    case class WorkDone(
      result: ReduceResult[Reducer.this.job.K, Reducer.this.job.V]
    )

  }

  /** Local reducer worker which execute the reduce job on tasks. */
  class Worker extends Actor with utils.debugs.Debug {

    this.setDebug(true)

    /** @see akka.actor.Actor.receive() */
    def receive: Receive = this.free

    /** State in which the worker is waiting for a task to perform. */
    def free: Receive = {

      case Reducer.Perform(task) =>
        context become this.waitingFiles(task, sender)
        Reducer.this.rfh ! RemoteFileHandler.GetDataFor(task.chunks)

    }

    /** State in which the worker wait the confirmation from the RFH that its
      * current task chunks are locally available.
      */
    def waitingFiles(task: Task, reducer: ActorRef): Receive = {

      case RemoteFileHandler.AvailableDataFor(localChunks) =>
        debugReceive("RemoteFileHandler.FilesAvailable", sender, "waitingFiles")
        context become this.busy(reducer)
        debugSend("Perform (self)", "waitingFiles")
        self ! Reducer.Perform(TaskBuilder.buildTask(task.key, localChunks))

    }

    /** State in which the worker is performing a task. */
    def busy(reducer: ActorRef): Receive = {

      case Reducer.Perform(task) =>
        debugReceive("Perform for a data of size " + task.nbValues, sender, "busy")
        new Thread(new ReducerRunnableTask(task, reducer)).start()
        context become this.free

    }

    /** Thread to perform the task, job and task are provided at construction
      *
      * @param task   performed task
      * @param sender reducer that created the worker
      */
    private class ReducerRunnableTask(
      task: Task,
      sender: ActorRef
    ) extends Runnable {

      def run(): Unit = {
        val result = Reducer.this.job reduce this.task

        this.sender ! Worker.WorkDone(result)
      }

    }

  }

}
