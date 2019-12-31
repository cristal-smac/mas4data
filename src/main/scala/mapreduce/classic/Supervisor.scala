package mapreduce.classic

import java.io.{BufferedWriter, File}

import akka.actor.{Actor, ActorRef, Address, AddressFromURIString, Deploy, Props}
import akka.remote.RemoteScope
import mapreduce.adaptive.Monitor
import mapreduce.filesystem.RemoteFileHandler
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.jobs._
import utils.tasks._

import scala.collection.mutable
import scala.io.Source

/** Companion object of the Supervisor agent.
  *
  * Contain all the messages that a supervisor could receive.
  */
object Supervisor {

  /** Message MapperFailure
    *
    * Inform the supervisor that a mapper failed.
    */
  case object MapperFailure

  /** Message MapperDone
    *
    * Inform the supervisor that a mapper has done its task.
    */
  case object MapperDone

  /** Message ReducerFailure
    *
    * Inform the supervisor that a reducer failed.
    */
  case object ReducerFailure

  /** Message ReducerDone
    *
    * Inform the supervisor that a reducer has done its task.
    */
  case object ReducerDone

  /** Message KillOk
    *
    * Acknowledgment for the Kill message.
    */
  case object KillOk

}

/** Represent a supervisor in a classic map reduce system. */
class Supervisor extends Actor with utils.debugs.Debug {

  import Supervisor._

  this.setDebug(true)

  // Timers
  var t0: Long = System.currentTimeMillis()
  var t1, t2: Long = 0

  private val config = ConfigurationBuilder.config

  // Amount of mapper for the system
  private val mappersAmount = this.config.nbMapper

  // Amount of reducer for the system
  private val reducersAmount = this.config.nbReducer

  // Reducers execution time
  private val reducerTimes = mutable.Map.empty[String, Long]

  // Create a new reduce task
  private def createNewReduceTask: ReduceJob =
    this.config.reduceJob.getDeclaredConstructor().newInstance() match {
      case t: ReduceJob => t
      case _            => throw new RuntimeException("not a reduce task")
    }

  // Create a local reducer
  private def createLocalReducer(nb: Int, rfh: ActorRef): ActorRef =
    context.actorOf(
      Props(classOf[Reducer], this.createNewReduceTask, rfh),
      name = "reducer" + nb
    )

  // Create a remote reducer
  private def createRemoteReducer(
    nb: Int,
    address: Address,
    rfh: ActorRef
  ): ActorRef =
    context.actorOf(
      Props(
        classOf[Reducer], this.createNewReduceTask, rfh
      ).withDeploy(Deploy(scope = RemoteScope(address))),
      name = "reducer" + nb
    )

  // Create a remote RFH
  private def createRemoteRFH(nb: Int, creationAddress: Address): ActorRef =
    context.actorOf(
      Props(
        classOf[RemoteFileHandler], self
      ).withDeploy(Deploy(scope = RemoteScope(creationAddress))),
      name = "RFH" + nb
    )

  // Create local RFH
  private val localRFH = context.actorOf(
    Props(classOf[RemoteFileHandler], self),
    name = "localRFH"
  )

  // Map which associates an RFH to its remote address
  private val remoteAddressToRFH: Map[String, ActorRef] = {
    val remoteAddresses =
      (this.config.remoteReducers.toMap ++ this.config.remoteMappers.toMap)
      .keys
      .toList
      .distinct
      .zipWithIndex

    remoteAddresses.foldLeft(Map[String, ActorRef]()) {
      case (map, (remoteAddress, i)) =>
        val address = AddressFromURIString(remoteAddress)
        // Deploy the remote RFH
        val remoteRFH = this.createRemoteRFH(i, address)

        map + (remoteAddress -> remoteRFH)
    }
  }

  // ALl the RFH of the system (remotes and local)
  private val allRFH: List[ActorRef] =
    this.localRFH :: this.remoteAddressToRFH.values.toList.distinct

  // Remote RFH of the system
  private val remoteRFH: List[ActorRef] = this.allRFH diff List(this.localRFH)

  // Sends the config object to the remote RFH
  this.remoteRFH foreach {
    rfh =>
      rfh ! RemoteFileHandler.ConfigureEnvironment(
        this.config,
        ConfigurationBuilder.configFolder +
          ConfigurationBuilder.configSerializedConfigFileName,
        this.config.resultPath
      )
  }

  // Reducers of the system
  private var reducers: List[ActorRef] = _

  // Create a new map task
  private def createNewMapTask: MapJob =
    this.config.mapJob.getDeclaredConstructor().newInstance() match {
      case t: MapJob => t
      case _         => throw new RuntimeException("not a map task")
    }

  // Create a local mapper
  private def createLocalMapper(nb: Int, rfh: ActorRef): ActorRef =
    context.actorOf(
      Props(classOf[Mapper], this.createNewMapTask, this.reducers, rfh),
      name = "mapper" +  nb
    )

  // Create a remote mapper
  private def createRemoteMapper(
    nb: Int,
    address: Address,
    rfh: ActorRef
  ): ActorRef =
    context.actorOf(
      Props(
        classOf[Mapper], this.createNewMapTask, this.reducers, rfh
      ).withDeploy(Deploy(scope = RemoteScope(address))),
      name = "mapper" + nb
    )

  // Mappers of the system
  private var mappers: List[ActorRef] = _

  // Map which associates mappers to a chunk of the init file
  private def mappersAndLines: Map[ActorRef, Iterator[String]] = {
    val file = new File(this.config.initFile)
    val source = Source fromFile file
    val indexedLines = source.getLines.zipWithIndex

    source.close

    val mappersAndEmptyLines = (this.mappers map {
      mapper => (mapper, Iterator[String]())
    }).toMap

    indexedLines.foldLeft(mappersAndEmptyLines) {
      case (map, (line, index)) =>
        val mapper = this.mappers(index % this.mappers.length)

        map.updated(mapper, map(mapper) ++ Iterator(line))
    }
  }

  // Create a local chunk from lines
  private def linesToChunk(
    lines: Iterator[String],
    mapper: ActorRef
  ): Chunk = {
    val path = this.config.resultPath + mapper.path.name + ".txt"
    val dataSource = FileSource.createFileSource(path, lines)

    new Chunk(dataSource, 0, this.localRFH)
  }

  // Associate mappers and chunks if chunks already exist
  private def getMappersAndChunksWithChunks: Map[ActorRef, List[Chunk]] = {
    val chunksPath = this.config.initChunksPath.get
    val mappersChunks = (1 to this.config.initChunksNumber.get) map {
      i => "mapper" + i + ".txt"
    }

    // Right number of mappers or too many mappers
    if (mappersChunks.length <= this.config.nbMapper) {
      val mappersAndChunks =
        this.mappers.zipAll(mappersChunks, this.mappers.head, "")

      (mappersAndChunks map {
        case (mapper, chunkName) =>
          val path = chunksPath + chunkName
          val chunk =
            if (chunkName == "")
              List()
            else
              List(new Chunk(FileSource(path), 0, this.localRFH))

          (mapper, chunk)
      }).toMap
    }
    // Too many chunks
    else {
      mappersChunks.zipWithIndex.foldLeft(Map[ActorRef, List[Chunk]]()) {
        case (acc, (chunkName, i)) =>
          val mapper = this.mappers(i % this.config.nbMapper)
          val path = chunksPath + chunkName
          val chunk = new Chunk(FileSource(path), 0, this.localRFH)

          if (acc contains mapper) {
            acc.updated(mapper, chunk :: acc(mapper))
          } else {
            acc + (mapper -> List(chunk))
          }
      }
    }
  }

  // Associate mappers and chunks
  private def getMappersAndChunksWithoutChunks: Map[ActorRef, List[Chunk]] =
    this.mappersAndLines map {
      case (mapper, lines) => (mapper, List(linesToChunk(lines, mapper)))
    }

  private def getMappersAndChunks: Map[ActorRef, List[Chunk]] =
    if (this.config.initChunks) {
      this.getMappersAndChunksWithChunks
    } else {
      this.getMappersAndChunksWithoutChunks
    }

  // Pass in the waitProperKill state
  private def goToWaitProperKill(reducersToWait: Set[ActorRef]): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitProperKill(reducersToWait, timer)
      orElse this.handleDeprecatedMapperDone("waitProperKill")
      orElse this.handleDeprecatedReducerDone("waitProperKill")
      orElse this.handleUnexpected("waitProperKill")
    )
    reducersToWait foreach {
      _ ! Reducer.Kill
    }
    timer ! Timer.Start
  }

  def receive: Receive = {
    // No remote configuration to wait
    if (this.remoteRFH.isEmpty) {
      self ! Monitor.ConfigOk
    }

    this.waitConfigDeployment(this.remoteRFH) orElse
      this.handleUnexpected("waitConfigDeployment")
  }


  /** State in which the supervisor waits for the configuration to be well
    * deployed on the remote nodes.
    */
  def waitConfigDeployment(rfhsToWait: List[ActorRef]): Receive = {

    case Monitor.ConfigOk if (rfhsToWait contains sender) || (sender == self) =>
      val newRfhsToWait = rfhsToWait diff List(sender)

      // The Configuration is well deployed
      if (newRfhsToWait.isEmpty) {
        // Launch mappers and reducers
        this.reducers = {
          val (remoteOnes, cpt) =
            this.config.remoteReducers.foldLeft((List[ActorRef](), 0)) {
              case ((remotes, innerCpt), (stringAddress, nb)) =>
                val address = AddressFromURIString(stringAddress)
                val remoteRFH = this.remoteAddressToRFH(stringAddress)
                val newRemotes = for {
                  i <- 1 to nb
                } yield this.createRemoteReducer(
                  innerCpt + i,
                  address,
                  remoteRFH
                )

                (remotes ++ newRemotes, innerCpt + nb)
            }
          val localOnes = for {
            i <- (cpt + 1) to this.reducersAmount
          } yield this.createLocalReducer(i, this.localRFH)

          remoteOnes ++ localOnes
        }
        this.mappers = {
          val (remoteOnes, cpt) =
            this.config.remoteMappers.foldLeft((List[ActorRef](), 0)) {
              case ((remotes, innerCpt), (stringAddress, nb)) =>
                val address = AddressFromURIString(stringAddress)
                val remoteRFH = this.remoteAddressToRFH(stringAddress)
                val newRemotes = for {
                  i <- 1 to nb
                } yield this.createRemoteMapper(
                  innerCpt + i,
                  address,
                  remoteRFH
                )

                (remotes ++ newRemotes, innerCpt + nb)
            }
          val localOnes = for {
            i <- (cpt + 1) to this.mappersAmount
          } yield this.createLocalMapper(i, this.localRFH)

          remoteOnes ++ localOnes
        }
        this.getMappersAndChunks foreach {
          case (mapper, chunks) => mapper ! Mapper.Execute(chunks)
        }


        context become (
          this waitMappers Set[ActorRef]()
          orElse this.handleUnexpected("waitMappers")
        )
      }
      // It remains some configuration object to deploy
      else {
        context become (
          this.waitConfigDeployment(newRfhsToWait)
          orElse this.handleUnexpected("waitConfigDeployment")
        )
      }

  }

  /** State in which the supervisor wait its mappers to do their task.
    *
    * @param terminatedMappers mappers which have finished their task
    */
  def waitMappers(terminatedMappers: Set[ActorRef]): Receive = {

    case MapperDone =>
      val newTerminatedMappers = terminatedMappers + sender

      if (newTerminatedMappers.size == this.mappers.length) {
        this.mappers foreach { _ ! Mapper.Shutdown }

        debug("mappers finished, starts reducers")
        t1 = System.currentTimeMillis()
        debug("Mapping time: " + ((t1 - t0) / 1e3).toString + " s")
        this.reducers foreach { _ ! Reducer.Execute }
        context become (
          this.waitReducers(Set[ActorRef]())
          orElse this.handleDeprecatedMapperDone("waitReducers")
          orElse this.handleUnexpected("waitReducers")
        )
      } else {
        context become (
          this.waitMappers(newTerminatedMappers)
          orElse this.handleUnexpected("waitMappers")
        )
      }

  }

  /** State in which the supervisor wait its reducers to do their task.
    *
    * @param terminatedReducers reducers which have finished their task
    */
  def waitReducers(terminatedReducers: Set[ActorRef]): Receive = {

    case ReducerDone =>
      sender ! Reducer.ReducerDoneOk

      val newTerminatedReducers = terminatedReducers + sender
      val t = System.currentTimeMillis()

      this.reducerTimes.update(sender.path.name, t - this.t1)

      if (newTerminatedReducers.size == this.reducers.length) {
        this.goToWaitProperKill(this.reducers.toSet)
      } else {
        context become (
          this.waitReducers(newTerminatedReducers)
          orElse this.handleDeprecatedMapperDone("waitReducers")
          orElse this.handleUnexpected("waitReducers")
        )
      }

    case MapperDone => sender ! Mapper.Shutdown

  }

  /** State in which the supervisor waits for all the agents of the system to be
    * dead in order the shut down.
    */
  def waitProperKill(
    reducersToWait: Set[ActorRef], timer: ActorRef
  ): Receive = {

    case KillOk =>
      val newReducersToWait = reducersToWait - sender

      if (newReducersToWait.isEmpty) {
        timer ! Timer.Cancel

        val rfhs = this.remoteAddressToRFH.values.toList.distinct

        if (rfhs.isEmpty) {
          context stop self
        } else {
          this.localRFH ! RemoteFileHandler.GetResFiles(rfhs)
          context become (
            this.waitRepatriation(rfhs.length)
            orElse this.handleUnexpected("waitRepatriation")
          )
        }
      } else {
        context become (
          this.waitProperKill(newReducersToWait, timer)
          orElse this.handleDeprecatedMapperDone("waitProperKill")
          orElse this.handleDeprecatedReducerDone("waitProperKill")
          orElse this.handleUnexpected("waitProperKill")
        )
      }

    case Timer.Timeout if sender == timer =>
      this.goToWaitProperKill(reducersToWait)

  }

  /** State in which the supervisor wait for the results files repatriation. */
  def waitRepatriation(rfhMessagesToWait: Int): Receive = {

    case RemoteFileHandler.FilesAvailable if rfhMessagesToWait == 1 =>
      this.stop()

    case RemoteFileHandler.FilesAvailable =>
      context become (
        this.waitRepatriation(rfhMessagesToWait - 1)
        orElse this.handleUnexpected("waitRepatriation")
      )

  }

  /** Handle the deprecated MapperDone messages.
    *
    * @param state state in which the deprecated MapperDone message is received
    */
  def handleDeprecatedMapperDone(state: String): Receive = {

    case MapperDone =>
      debugDeprecated("MapperDone", sender, state)
      sender ! Mapper.Shutdown

  }

  /** Handle the deprecated ReducerDone messages.
    *
    * @param state state in which the deprecated ReducerDone message is received
    */
  def handleDeprecatedReducerDone(state: String): Receive = {

    case ReducerDone =>
      debugDeprecated("ReducerDone", sender, state)
      sender ! Reducer.ReducerDoneOk

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

    // ---------- PRINTS ---------- //

    t2 = System.currentTimeMillis()

    val sortedReducerTimes = this.reducerTimes.toList sortWith {
      case ((_, t1p), (_, t2p)) => t1p < t2p
    }
    val reducerTimesString = sortedReducerTimes map {
      case (reducer, t) => reducer + " : " + (t / 1e3).toString + " s"
    }
    val timeFairness =
      sortedReducerTimes.head._2.toDouble / sortedReducerTimes.last._2

    debug("Reducing time: " + ((this.t2 - this.t1) / 1e3).toString + " s" )
    debug("In detail:\n" + (reducerTimesString mkString "\n"))
    debug("Time fairness: " + timeFairness)

    // ---------- GNUPLOT SCRIPT ---------- //

    val file = new File(this.config.resultPath + "contribution.plot")
    val bw = new BufferedWriter(new java.io.FileWriter(file))
    val n = this.reducers.length

    val title = this.config.gnuplotTitle + "\\n"
      "job : " + this.config.jobName + "\\n"
      "pause = " + this.config.milliPause + "ms" + this.config.nanoPause + "ns"

    bw.write("set terminal '" + this.config.gnuplotOutputFormat + "'\n")
    bw.write(
      "set output '" +
        this.config.gnuplotOutputFilename + "." +
        this.config.gnuplotOutputFormat + "'\n"
    )
    bw.write("set title \"" + title + "\"\n")
    bw.write("set boxwidth 0.75\n")

    bw.write("set style data histograms\n")
    bw.write("set style histogram rowstacked\n")

    bw.write("set style fill solid\n")
    bw.write("set yrange [0:*]\n")

    bw.write("system('cat ")
    for (i <- 1 to n) {
      bw.write("reducer" + i + ".dat ")
    }
    bw.write("> all.data')\n")
    bw.write("system('cat worker@* > results')\n")

    bw.write(
      "plot for [COL=2:" +
        (this.config.gnuplotMaxTaskDoneNumber + 2) +
        "] 'all.data'  using COL:xticlabels(1) title ''\n"
    )

    bw.close()
  }

  /** Stop the supervisor. */
  def stop(): Unit = context stop self

}
