package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Props}
import mapreduce.filesystem.RemoteFileHandler
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.tasks.{Chunk, Task, TaskBuilder}
import utils.jobs.MapJob

/** Companion object of the AdaptiveMapper class.
  *
  * Contain all the messages that a mapper could receive.
  */
object AdaptiveMapper {

  /** Message Execute
    *
    * Ask to the mapper to execute its job on a given chunk.
    *
    * @param chunks      chunks on which execute the Task
    * @param partitioner partitioner of the system
    */
  case class Execute(chunks: List[Chunk], partitioner: ActorRef)

  /** Message PartitionOk
    *
    * Acknowledgment of the Partitioner.Partition message.
    *
    * @param index index of the task bundle which is received by the partitioner
    */
  case class PartitionOk(index: Int)

  /** Message Kill
    *
    * Ask to the mapper to shutdown.
    */
  case object Kill

}

/** Represent a mapper in an adaptive map reduce system.
  *
  * @param job map job to perform
  * @param rfh remote file handler of the mapper
  */
class AdaptiveMapper(
  job: MapJob,
  rfh: ActorRef
) extends Actor with utils.debugs.Debug {

  import AdaptiveMapper._

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-mapper"))

  // Pass in the waitReducers state
  private def goToWaitPartitioner(
    partitioner: ActorRef,
    toSend: List[Task],
    tasks: Iterator[List[Task]],
    index: Int,
    state: String
  ): Unit = {
    val timer = context actorOf Props(
      classOf[Timer], this.config.timeouts("acknowledgment-timeout")
    )

    context become (
      this.waitPartitioner(partitioner, toSend, tasks, index, timer)
      orElse this.handleDeprecatedPartitionOkWhileSending(
        "waitPartitioner",
        index
      ) orElse this.handleUnexpected("waitPartitioner")
    )
    debugSend("Partitioner.Partition", state)
    partitioner ! Partitioner.Partition(toSend, index, !tasks.hasNext)
    timer ! Timer.Start
  }

  def receive: Receive =
    this.waitExecution orElse this.handleUnexpected("waitExecution")

  /** State in which the mapper waits a task to execute. */
  def waitExecution: Receive = {

    case Execute(chunks, partitioner) if chunks.nonEmpty =>
      debugReceive("Execute", sender, "waitExecution")
      context become (
        this.waitChunks(chunks, partitioner)
        orElse this.handleDeprecatedExecuteFromMonitor("execution")
        orElse this.handleUnexpected("execution")
      )
      debugSend("Monitor.ExecuteOk", "waitExecution")
      context.parent ! Monitor.ExecuteOk
      this.rfh ! RemoteFileHandler.GetDataFor(chunks)

    case Execute(_, partitioner) =>
      debugReceive("Execute", sender, "waitExecution")
      debugSend("Monitor.ExecuteOk", "waitExecution")
      context.parent ! Monitor.ExecuteOk
      this.goToWaitPartitioner(partitioner, Nil, Iterator(), 1, "waitExecution")

  }

  /** State in which the mapper waits for the RFH green light.
    *
    * @param chunks      chunks asked to the RFH
    * @param partitioner partitioner agent to deal with
    */
  def waitChunks(chunks: List[Chunk], partitioner: ActorRef): Receive = {

    case RemoteFileHandler.AvailableDataFor(localChunks) =>
      debugReceive("RemoteFileHandler.AvailableDataFor", sender, "waitChunks")
      context become (
        this.execution
        orElse this.handleDeprecatedExecuteFromMonitor("execution")
        orElse this.handleUnexpected("execution")
      )
      self ! Execute(localChunks, partitioner)

  }

  /** State in which the mapper executes a task. */
  def execution: Receive = {

    case Execute(chunks, partitioner) if sender == self =>
      debugReceive("Execute from itself", sender, "execution")

      var lineNumber = 0
      val lines = chunks.foldLeft(Iterator[String]()) {
        case (acc, chunk) => acc ++ (chunk.get filterNot { _.isEmpty })
      }

      lines foreach { line =>
        lineNumber += 1

        if (lineNumber % 1000000 == 0) {
          debug(s"processes $lineNumber lines")
        }

        this.job.produceMapResult(line, self, this.rfh)
      }

      val results = this.job.getResults(self, this.rfh)
      val tasks = (results map {
        case (key, keyChunks) => TaskBuilder.buildTask(key.toString, keyChunks)
      }).toList

      if (tasks.nonEmpty) {
        val groupedTasks = tasks.grouped(1000)
        val toSend = groupedTasks.next

        this.goToWaitPartitioner(
          partitioner,
          toSend,
          groupedTasks,
          1,
          "execution"
        )
      } else {
        this.goToWaitPartitioner(partitioner, Nil, Iterator(), 1, "execution")
      }

  }

  /** State in which the mapper waits the acknowledgments from the partitioner.
    *
    * @param partitioner partitioner to deal with
    * @param justSent    task bundle which was just sent to the partitioner
    * @param tasks       task sent to the partitioner
    * @param index       index of tasks to send to the partitioner
    * @param timer       current timer
    */
  def waitPartitioner(
    partitioner: ActorRef,
    justSent: List[Task],
    tasks: Iterator[List[Task]],
    index: Int,
    timer: ActorRef
  ): Receive = {

    case PartitionOk(i)
      if sender == partitioner &&
      i == index &&
      !tasks.hasNext =>
      debugReceive("Partitioner.PartitionOk", sender, "waitPartitioner")
      timer ! Timer.Cancel
      context become (
        this.waitSupervisor
          orElse this.handleDeprecatedPartitionOk("waitSupervisor")
          orElse this.handleUnexpected("waitSupervisor")
      )

    case PartitionOk(i)
      if sender == partitioner && i == index =>
      debugReceive("Partitioner.PartitionOk", sender, "waitPartitioner")
      timer ! Timer.Cancel
      this.goToWaitPartitioner(
        partitioner,
        tasks.next,
        tasks,
        index + 1,
        "waitPartitioner"
      )

    case Timer.Timeout if sender == timer =>
      debugReceive("Timer.Timeout", sender, "waitPartitioner")
      this.goToWaitPartitioner(
        partitioner,
        justSent,
        tasks,
        index,
        "waitPartitioner"
      )

  }

  /** State in which the mapper waits the signal of the supervisor to shut down.
    */
  def waitSupervisor: Receive = {

    case Kill =>
      debugReceive("Kill", sender, "waitSupervisor")
      context stop self

  }

  /** Handle the deprecated Execute message from the monitor.
    *
    * @param state state in which the deprecated Execute message is received
    */
  def handleDeprecatedExecuteFromMonitor(state: String): Receive = {

    case Execute if sender == context.parent =>
      debugDeprecated("Execute from Monitor", sender, state)
      sender ! Monitor.ExecuteOk

  }

  /** Handle deprecated PartitionOk messages while sending Partition messages.
    *
    * @param state state in which the deprecated WorkOnOk message is received
    */
  def handleDeprecatedPartitionOkWhileSending(
    state: String,
    index: Int
  ): Receive = {

    case PartitionOk(i) if i == index - 1 =>
      debugDeprecated("PartitionOk(" + i + ")", sender, state)

  }

  /** Handle deprecated WorkOnOk messages.
    *
    * @param state state in which the deprecated WorkOnOk message is received
    */
  def handleDeprecatedPartitionOk(state: String): Receive = {

    case PartitionOk(i) =>
      debugDeprecated("PartitionOk(" + i + ")", sender, state)

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
    * @param state state in which the unexpected messages is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}
