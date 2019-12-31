package mapreduce.classic

import akka.actor.{Actor, ActorRef, Props}
import mapreduce.filesystem.RemoteFileHandler
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.jobs.MapJob
import utils.tasks.{Chunk, Task, TaskBuilder}

/** Companion object of the Mapper class.
  *
  * Contain all the messages that a mapper could receive.
  */
object Mapper {

  /** Message Execute
    *
    * Ask to the mapper to execute its job on given chunks.
    *
    * @param chunks chunks on which execute the job
    */
  case class Execute(chunks: List[Chunk])

  /** Message WorkOnOK
    *
    * Inform the mapper that a reducer has well received its results.
    */
  case object WorkOnOK

  /** Message Shutdown
    *
    * Ask to the mapper to shutdown.
    */
  case object Shutdown

}

/** Represent a mapper in a classic map reduce system.
  *
  * @param job      map job that the mapper has to execute
  * @param reducers reducers of the MapReduce process
  * @param rfh      remote file handler which deals with this mapper
  */
class Mapper(
  job: MapJob,
  reducers: List[ActorRef],
  rfh: ActorRef
) extends Actor with utils.debugs.Debug {

  import Mapper._

  private val config = ConfigurationBuilder.config

  // Pass in the waitReducers state
  private def goToWaitReducersState(
    reducersAndTasks: Map[ActorRef, List[Task]]
  ): Unit = {
    val timer = context actorOf Props(
      classOf[Timer], this.config.timeouts("acknowledgment-timeout")
    )

    context become (
      this.waitReducers(reducersAndTasks, timer)
      orElse handleDeprecatedWorkOnOk("waitReducers")
      orElse this.handleUnexpected("waitReducers")
    )
    reducersAndTasks foreach {
      case (reducer, tasks) => reducer ! Reducer.WorkOn(tasks)
    }
    timer ! Timer.Start
  }

  // Pass in the waitSupervisor state
  def goToWaitSupervisorState(): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitSupervisor(timer)
      orElse this.handleDeprecatedWorkOnOk("waitSupervisor")
      orElse this.handleDeprecatedTimeout("waitSupervisor")
      orElse this.handleUnexpected("waitSupervisor")
    )
    context.parent ! Supervisor.MapperDone
    timer ! Timer.Start
  }

  def receive: Receive =
    this.waitExecution orElse this.handleUnexpected("waitExecution")

  /** State in which the mapper wait the green light from the supervisor to
    * execute its job.
    */
  def waitExecution: Receive = {

    case Execute(chunks) if chunks.nonEmpty =>
      context become (
        this.waitChunks(chunks)
        orElse this.handleUnexpected("execution")
      )
      this.rfh ! RemoteFileHandler.GetDataFor(chunks)

    case Execute(_) =>
      this.goToWaitSupervisorState()

  }

  /** State in which the mapper wait the confirmation from the RFH that its
    * chunks are locally available.
    */
  def waitChunks(chunks: List[Chunk]): Receive = {

    case RemoteFileHandler.AvailableDataFor(localChunks) =>
      debugReceive("RemoteFileHandler.FilesAvailable", sender, "waitChunk")
      context become (
        this.execution
        orElse this.handleUnexpected("execution")
      )
      self ! Execute(localChunks)

  }

  /** State in which the mapper execute its task. */
  def execution: Receive = {

    case Execute(chunks) if sender == self =>
      debugReceive("Execute from itself", sender, "execution")

      var lineNumber = 0
      val lines = chunks.foldLeft(Iterator[String]()) {
        case (acc, chunk) => acc ++ (chunk.get filterNot { _.isEmpty })
      }

      lines foreach { line =>
        lineNumber += 1

        if (lineNumber % 1000000 == 0) {
          debug("execute line " + lineNumber)
        }

        this.job.produceMapResult(line, self, this.rfh)
      }

      // Map[K, List[Chunk]]
      val results = this.job.getResults(self, this.rfh)
      val reducerAmount = this.reducers.length
      // Map[ActorRef, List[(K, List[Chunk])]]
      val resultsByReducer = results.toList groupBy {
        case (key, _) =>
          this.reducers(mapreduce.partition(key, reducerAmount))
      }
      // Map[ActorRef, List[Task]]
      val reducersAndTasks = resultsByReducer map {
        case (reducer, notYetCreatedTasks) =>
          val tasks = notYetCreatedTasks map {
            case (key, keyChunks) =>
              TaskBuilder.buildTask(key.toString, keyChunks)
          }

          reducer -> tasks
      }

      // Send valued chunks to the reducers
      reducersAndTasks foreach {
        case (reducer, tasks) => reducer ! Reducer.WorkOn(tasks)
      }
      this goToWaitReducersState reducersAndTasks

  }

  /** State in which the mapper wait the acknowledgments from the reducers.
    *
    * @param reducersAndTasks map which associate reducers and tasks
    * @param timer            current timer
    */
  def waitReducers(
    reducersAndTasks: Map[ActorRef, List[Task]],
    timer: ActorRef
  ): Receive = {

    case WorkOnOK if reducersAndTasks contains sender =>
      val newReducersAndTasks = reducersAndTasks - sender

      if (newReducersAndTasks.isEmpty) {
        timer ! Timer.Cancel
        this.goToWaitSupervisorState()
      } else {
        context become (
          this.waitReducers(newReducersAndTasks, timer)
          orElse this.handleDeprecatedWorkOnOk("waitReducers")
          orElse this.handleUnexpected("waitReducers")
        )
      }

    case Timer.Timeout if sender == timer =>
      this.goToWaitReducersState(reducersAndTasks)

  }

  /** State in which the mapper wait the signal of the supervisor to shut down.
    *
    * @param timer current timer
    */
  def waitSupervisor(timer: ActorRef): Receive = {

    case Shutdown =>
      timer ! Timer.Cancel
      context stop self

    case Timer.Timeout if sender == timer => this.goToWaitSupervisorState()

  }

  /** Handle the deprecated Timeout messages.
    *
    * @param state state in which the deprecated Timeout message is received
    */
  def handleDeprecatedTimeout(state: String): Receive = {

    case Timer.Timeout =>
      debugDeprecated("Timer.Timeout", sender, state)

  }

  /** Handle the deprecated WorkOnOk messages.
    *
    * @param state state in which the deprecated WorkOnOk message is received
    */
  def handleDeprecatedWorkOnOk(state: String): Receive = {

    case WorkOnOK =>
      debugDeprecated("WorkOnOk", sender, state)

  }

  /** Handle the unexpected message.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case _@msg => debugUnexpected(self, sender, state, msg)

  }

}
