package mapreduce.adaptive

import akka.actor.{Actor, ActorRef, Address, AddressFromURIString, Deploy, Props}
import akka.remote.RemoteScope
import java.io.{BufferedWriter, File}

import scala.collection.mutable
import scala.io.Source
import mapreduce.filesystem.RemoteFileHandler
import mapreduce.time.Timer
import utils.config.ConfigurationBuilder
import utils.experiments.{Archive, MonitorData, ReducerData}
import utils.files.FileWriter
import utils.tasks.{Chunk, FileSource}
import utils.jobs.{MapJob, ReduceJob}

/** Companion object of the Monitor class.
  *
  * Contain all the messages that a monitor could receive.
  */
object Monitor {

  /** Reducer name prefix, will be post fixed by reducer's number. */
  val reducerNamePrefix = "reducer"

  /** Mapper name prefix, will be post fixed by mapper's number. */
  val mapperNamePrefix = "mapper"

  // ----- REMOTE FILE HANDLER ----- //

  case object ConfigOk

  // ----- MAPPERS AND REDUCERS ----- //

  /** Message ExecuteOk
    *
    * Acknowledgment for the Execute message.
    */
  case object ExecuteOk

  // ----- PARTITIONER ----- //

  /** Message StartPartitioning
    *
    * Inform the monitor that the partitioner starts the task partitioning.
    */
  object StartPartitioning

  /** Message PartitionDone
    *
    * Inform the monitor that the partitioner has done its job.
    */
  object PartitionDone

  // ----- REDUCER ----- //

  /** Message GetRFH.
    *
    * Message that a reducer sends to the monitor in order to give information
    * about its RFH.
    *
    * @param rfh RFH actor reference
    */
  case class GetRFH(rfh: ActorRef)

  /** Message BroadcastRFHOk.
    *
    * Message that a reducer sends to inform the monitor that it knows the
    * reducer distribution among RFH.
    */
  case object BroadcastRFHOk

  /** Message AcquaintancesOk
    *
    * Acknowledgment for the Acquaintances message.
    */
  case object AcquaintancesOk

  /** Message ReducerActive
    *
    * Tell to the monitor that a reducer is active.
    *
    * @param reducerName name of the reducer becoming active
    */
  case class ReducerActive(reducerName: String)

  /** Message ReducerIdle
    *
    * Tell to the monitor that a reducer is inactive.
    *
    * @param reducerName name of the reducer becoming idle
    */
  case class ReducerIdle(reducerName: String, idleTime: Long)

  /** Message KillOk
    *
    * Acknowledgment of the Kill message.
    *
    * @param reducerData data produced by the reducer during a run
    */
  case class KillOk(reducerData: ReducerData)

  /** Message ReadyToDieAnswer.
    *
    * Answer to the ReadyToDie message.
    *
    * @param id    identifier of the attempt to end the run
    * @param ready true iff the reducer is ready to die
    */
  case class ReadyToDieAnswer(id: Int, ready: Boolean)

  // ---------- REMOTE FILE HANDLER ---------- //

  /** Message ResultsRemoved
    *
    * Inform the monitor that the RFH removed the results of the current run.
    */
  case object ResultsRemoved

}

/** Monitor of the map reduce system. */
class Monitor[MK, MV, K, V] extends Actor with utils.debugs.Debug {

  import Monitor._

  // Timers
  val t0: Long = System.currentTimeMillis()
  var t1, t2: Long = 0.toLong

  private val config = ConfigurationBuilder.config

  this.setDebug(this.config.debugs("debug-monitor"))

  debug("works on problem " + this.config.jobName)

  // Create a reduce job regarding configuration
  private def createNewReduceJob: ReduceJob =
    this.config.reduceJob.getDeclaredConstructor().newInstance() match {
      case t: ReduceJob => t
      case _            => throw new RuntimeException("not a reduce task")
    }

  // Create a local reducer with monitor
  private def createLocalReducerWithMonitor(nb: Int, rfh: ActorRef): ActorRef =
    context.actorOf(
      Props(
        classOf[AdaptiveReducerWithMonitor],
        this.createNewReduceJob,
        rfh
      ),
      name = reducerNamePrefix + nb
    )

  // Create a local reducer without a monitor
  private def createLocalReducer(nb: Int, rfh: ActorRef): ActorRef =
    context.actorOf(
      Props(
        classOf[AdaptiveReducer],
        this.createNewReduceJob,
        rfh
      ),
      name = reducerNamePrefix + nb
    )

  // Create a remote reducer with a monitor
  private def createRemoteReducerWithMonitor(
    nb: Int,
    address: Address,
    rfh: ActorRef
  ): ActorRef = context.actorOf(
    Props(
      classOf[AdaptiveReducerWithMonitor],
      this.createNewReduceJob,
      rfh
    ).withDeploy(Deploy(scope = RemoteScope(address))),
    name = reducerNamePrefix + nb
  )

  // Create a remote reducer without a monitor
  private def createRemoteReducer(
    nb: Int,
    address: Address,
    rfh: ActorRef
  ): ActorRef =
    context.actorOf(
      Props(
        classOf[AdaptiveReducer],
        this.createNewReduceJob,
        rfh
      ).withDeploy(Deploy(scope = RemoteScope(address))),
      name = reducerNamePrefix + nb
    )

  // Create a remote file handler
  private def createRemoteRFH(nb: Int, creationAddress: Address): ActorRef =
    context.actorOf(
      Props(
        classOf[RemoteFileHandler]
      ).withDeploy(Deploy(scope = RemoteScope(creationAddress))),
      name = "RFH" + nb
    )

  // Create local RFH
  private val localRFH = context.actorOf(
    Props(classOf[RemoteFileHandler]),
    name = "localRFH"
  )

  // Associate a remote address with its RFH
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

  // List of all the RFH in the system
  private val allRFH: List[ActorRef] =
    this.localRFH :: this.remoteAddressToRFH.values.toList.distinct

  // List of all the remote RFH of the system
  private val remoteRFH: List[ActorRef] = this.allRFH diff List(this.localRFH)

  // Sends the config object to the remote RFH
  this.remoteRFH foreach { rfh =>
    rfh ! RemoteFileHandler.ConfigureEnvironment(
      this.config,
      ConfigurationBuilder.configFolder +
        ConfigurationBuilder.configSerializedConfigFileName,
      this.config.resultPath
    )
  }

  // Partitioner of the system
  private var partitioner: ActorRef = _

  // Reducers (remote and local) of the system
  private var reducers: List[ActorRef] = _

  private def getSeed(nb: Int): Option[Int] =
    if (this.config.mapperSeeds.isDefined) {
      Some(this.config.mapperSeeds.get(nb - 1))
    } else None

  // Create a map job regarding configuration
  private def createNewMapJob(nb: Int): MapJob =
    this.config.mapJob.getDeclaredConstructor().newInstance() match {
      case t: MapJob => t.seed = this.getSeed(nb); t.mapperNb = nb; t
      case _         => throw new RuntimeException("not a map task")
    }

  // Create a local mapper
  private def createLocalMapper(nb: Int, rfh: ActorRef): ActorRef =
    context.actorOf(
      Props(
        classOf[AdaptiveMapper], this.createNewMapJob(nb), rfh
      ),
      name = mapperNamePrefix + nb
    )

  // Create a remote mapper
  private def createRemoteMapper(
    nb: Int,
    address: Address,
    rfh: ActorRef
  ): ActorRef =
    context.actorOf(
      Props(
        classOf[AdaptiveMapper], this.createNewMapJob(nb), rfh
      ).withDeploy(Deploy(scope = RemoteScope(address))),
      name = mapperNamePrefix + nb
    )

  // Mappers (remote and local) of the system
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
      val mappersAndChunks = this.mappers.zipAll(
        mappersChunks,
        this.mappers.head,
        ""
      )

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

  // Get the reducer number from the reducer name
  private def getReducerNumberFromReducerName(reducerName: String): String =
    reducerName.substring(reducerNamePrefix.length)

  // Reducers execution time
  private val reducerTimes = mutable.Map.empty[String, Long]

  // Send its acquaintances to a given reducer
  // We consider here reducers are fully connected
  private def sendAcquaintances(reducer: ActorRef): Unit = {
    reducer ! AdaptiveReducer.Acquaintances(this.reducers.toSet - reducer)
  }

  // Pass in the waitMappers state
  private def goToWaitMappers(
    mappersToWait: Map[ActorRef, List[Chunk]]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitMappers(mappersToWait, timer)
      orElse this.handleDeprecatedGetRFH("waitMappers")
      orElse this.handleDeprecatedRFHBroadcastOk("waitMappers")
      orElse this.handleUnexpected("waitMappers")
    )
    mappersToWait foreach {
      case (mapper, chunks) =>
        mapper ! AdaptiveMapper.Execute(chunks, this.partitioner)
    }
    timer ! Timer.Start
  }

  // Pass in the waitReducers state
  private def goToWaitReducersState(reducersToWait: Set[ActorRef]): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitReducersState(reducersToWait, timer)
      orElse this.handleDeprecatedTimeout("waitReducersState")
      orElse this.handleUnexpected("waitReducersState")
    )
    reducersToWait foreach this.sendAcquaintances
    timer ! Timer.Start
  }

  // Pass in the waitReducersExecution state
  private def goToWaitReducersExecution(reducersToWait: Set[ActorRef]): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitReducersExecution(reducersToWait, timer)
      orElse this.active(
        this.reducers.toSet,
        this.reducers.map {
          r => this.getReducerNumberFromReducerName(r.path.name)
        },
        0
      )
      orElse this.handleDeprecatedAcquaintancesOk(
        "waitReducersExecution/active"
      )
      orElse this.handleDeprecatedTimeout("waitReducersExecution/active")
      orElse this.handleUnexpected("waitReducersExecution/active")
    )
    reducersToWait foreach { reducer =>
      debugSend("AdaptiveReducer.Execute", "waitReducersExecution")
      reducer ! AdaptiveReducer.Execute
    }
    timer ! Timer.Start
  }

  // Pass in the waitProperKill state
  private def goToWaitProperKill(
    reducersToWait: Set[ActorRef],
    reducersData: Set[ReducerData]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    context become (
      this.waitProperKill(timer, reducersToWait, reducersData)
      orElse this.handleDeprecatedReducerIdle("waitProperKill")
      orElse this.handleDeprecatedReadyToDieAnswer(0, "waitProperKill")
      orElse this.handleUnexpected("waitProperKill")
    )
    reducersToWait foreach { _ ! AdaptiveReducer.Kill }
    timer ! Timer.Start
  }

  // Pass in the askRFHToReducers state
  private def goToAskRFHToReducers(
    reducersToWait: Set[ActorRef],
    rfhMap: Map[ActorRef, ActorRef]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    reducersToWait foreach { _ ! AdaptiveReducer.GetRFH }

    context become (
      this.askRFHToReducers(reducersToWait, rfhMap, timer)
      orElse handleDeprecatedGetRFH("askRFHToReducers")
      orElse handleUnexpected("askRFHToReducers")
    )
  }

  // Pass in the broadcastRFH state
  private def goToBroadcastRFH(
    reducersToWait: Set[ActorRef],
    rfhMap: Map[ActorRef, ActorRef]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    reducersToWait foreach {
      reducer => reducer ! AdaptiveReducer.BroadcastRFH(rfhMap)
    }

    context become (
      this.broadcastRFH(reducersToWait, rfhMap, timer)
      orElse this.handleDeprecatedRFHBroadcastOk("broadcastRFH")
      orElse this.handleUnexpected("broadcastRFH")
    )

  }

  // Pass in the waitKillConfirmation state
  def goToWaitKillConfirmation(
    attemptToEndId: Int,
    reducersToWaitFor: Set[ActorRef]
  ): Unit = {
    val timer = context.actorOf(
      Props(classOf[Timer], this.config.timeouts("acknowledgment-timeout"))
    )

    reducersToWaitFor foreach {
      reducer => reducer ! Manager.ReadyToDie(attemptToEndId)
    }
    context become (
      this.waitKillConfirmation(attemptToEndId, reducersToWaitFor, timer)
      orElse this.handleDeprecatedExecuteOk("waitKillConfirmation")
      orElse this.handleDeprecatedTimeout("waitKillConfirmation")
      orElse this.handleDeprecatedReadyToDieAnswer(
        attemptToEndId,
        "waitKillConfirmation"
      )
    )

  }

  // Kill the monitor
  private def kill(reducersData: List[ReducerData]): Unit = {

    // ---------- MONITOR DATA ---------- //

    val reducingTime = (this.t2 - this.t1) / 1e3
    val sortedReducerTimes = this.reducerTimes.toList sortWith {
      case ((_, t1p), (_, t2p)) => t1p < t2p
    }
    val timeFairness =
      sortedReducerTimes.head._2.toDouble / sortedReducerTimes.last._2
    val monitorData = new MonitorData(reducersData, reducingTime, timeFairness)

    monitorData.write(this.config.resultPath + "monitor.csv")

    // ---------- CFP TIMELINE ---------- //

    val resultsFiles = new File(this.config.resultPath).list
    val cfpArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") && (fileName endsWith "CFP.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val cfpArchives = cfpArchivesFileNames map { archiveFileName =>
      new Archive[(String, Int, Int), (Int, Int, Int)](
        "",
        archiveFileName,
        true,
        {
          line =>
            val splitLine = line split ";"

            (
              splitLine(0).toLong,
              (
                splitLine(1),
                splitLine(2).toInt,
                splitLine(3).toInt
              )
            )
        }
      )
    }

    if (!cfpArchives.isEmpty) {
      val globalCfpArchive = cfpArchives.tail.foldLeft(cfpArchives.head) {
        case (acc, archive) => acc.fuseWith(archive, "CFP_global")
      }

      globalCfpArchive.writeTimelineCSV(
        "Number of CFP;Number of Propose;Number of Decline",
        this.t1,
        1000,
        { records => records.foldLeft((0.toLong, (0, 0, 0))) {
          case (
            (_, (nbCfp, nbPropose, nbDecline)),
            (t2p, (_, proposes, declines))
          ) =>
            (t2p, (nbCfp + 1, nbPropose + proposes, nbDecline + declines))
        } },
        true,
        { case (cfps, proposes, declines) =>
          cfps + ";" + proposes + ";" + declines
        }
      )
    }

    // ---------- FAIRNESS TIMELINE ---------- //

    val contributionArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
        (fileName endsWith "contribution.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val contributionArchives = contributionArchivesFileNames map {
      archiveFileName =>
        new Archive[(String, Long), (Double, Long, Long, Double)](
          "",
          archiveFileName,
          true,
          {
            line =>
              val splitLine = line split ";"

              (splitLine(0).toLong, (splitLine(1), splitLine(2).toLong))
          }
        )
    }

    if (!contributionArchives.isEmpty) {
      val globalContributionArchive =
        contributionArchives.tail.foldLeft(contributionArchives.head) {
          case (acc, archive) => acc.fuseWith(archive, "fairness_global")
        }
      val managerIds = this.reducers map {
        reducer => "manager@" + reducer.path.name
      }
      val oldContrib: mutable.Map[String, Long] = {
        val m = mutable.Map[String, Long]()

        managerIds.foldLeft(m) { case (acc, id) => acc + (id -> 0) }
      }

      globalContributionArchive.writeTimelineCSV(
        "Fairness;Contribution min.;Contribution max.;Average contribution",
        this.t1,
        1000,
        { records =>
          val (timestamp, contributions) =
            records.foldLeft((0.toLong, Map[String, Long]())) {
              case ((_, ac), (t2p, (id, contrib))) =>
                val nac = if (ac contains id) {
                  ac.updated(id, contrib)
                } else {
                  ac + (id -> contrib)
                }

                oldContrib.update(id, contrib)
                (t2p, nac)
            }
          val allContributions = oldContrib.foldLeft(contributions) {
            case (ac, (id, oc)) => if (ac contains id) ac else ac + (id -> oc)
          }
          val values = allContributions.values.toList
          val max = values.max
          val min = values.min
          val mean = values.sum.toDouble / values.length
          val fairness = if (max == 0) 0 else min.toDouble / max

          (timestamp, (fairness, min, max, mean))
        },
        true,
        { case (f, min, max, mean) => f + ";" + min + ";" + max + ";" + mean }
      )
    }

    // ---------- NB VALUES FAIRNESS TIMELINE ---------- //

    val nbValuesArchiveFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
        (fileName endsWith "nbValues.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val nbValuesArchives = nbValuesArchiveFileNames map {
      archiveFileName => new Archive[(String, Int), (Double, Int, Int)](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, (splitLine(1), splitLine(2).toInt))
        }
      )
    }

    if (nbValuesArchives.nonEmpty) {
      val globalNbValuesArchive = nbValuesArchives.tail.foldLeft(nbValuesArchives.head) {
        case (acc, archive) => acc.fuseWith(archive, "nbValues_global")
      }
      val managerIds = this.reducers.map {
        reducer => "manager@" + reducer.path.name
      }
      val oldnbValues: mutable.Map[String, Int] = {
        val m = mutable.Map[String, Int]()

        managerIds.foldLeft(m) { case (acc, id) => acc + (id -> 0) }
      }

      globalNbValuesArchive.writeTimelineCSV(
        "Costs fairness; Costs min.; Costs max.",
        this.t1,
        1000,
        { records =>
          // Update nbValues
          records foreach {
            case (_, (id, cost)) => oldnbValues.update(id, oldnbValues(id) + cost)
          }

          val max = oldnbValues.values.max
          val min = oldnbValues.values.min
          val fairness = if (max == 0) 0 else min.toDouble / max

          (0.toLong, (fairness, min, max))
        },
        true,
        { case (f, min, max) => f + ";" + min + ";" + max }
      )
    }

    // ----------  DELEGATION COSTS FAIRNESS TIMELINE ---------- //

    val delegationCostsArchiveFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
          (fileName endsWith "delegation_cost.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val delegationCostsArchives = delegationCostsArchiveFileNames map {
      archiveFileName => new Archive[Long, Double](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, splitLine(1).toLong)
        }
      )
    }

    if (delegationCostsArchives.nonEmpty) {
      val globalDelegationCostsArchive: Archive[Long, Double] =
        delegationCostsArchives.tail.foldLeft(delegationCostsArchives.head) {
          case (acc, archive) =>
            acc.fuseWith(archive, "delegation_costs_global")
        }

      globalDelegationCostsArchive.writeTimelineCSV(
        "Mean delegation cost",
        this.t1,
        1000,
        { records =>
          val (time, sum) = records.foldLeft((0.toLong, 0.toLong)) {
            case ((_, totalDelegatedCost), (t, x)) =>
              (t, totalDelegatedCost + x)
          }

          (time, sum.toDouble / records.length)
        },
        true,
        { _.toString }
      )
    }

    // ---------- PAUSE STATE TIMELINE ---------- //

    val pauseArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
        (fileName endsWith "pause.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val pauseArchives = pauseArchivesFileNames map {
      archiveFileName => new Archive[(String, Boolean), Int](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, (splitLine(1), splitLine(2).toBoolean))
        }
      )
    }

    if (!pauseArchives.isEmpty) {
      val globalPauseArchive = pauseArchives.tail.foldLeft(pauseArchives.head) {
        case (acc, archive) => acc.fuseWith(archive, "pause_global")
      }
      val managerIds = this.reducers map {
        reducer => "manager@" + reducer.path.name
      }
      val pauseState = {
        val m = mutable.Map[String, Boolean]()

        managerIds.foldLeft(m) { case (acc, id) => acc + (id -> false) }
      }

      globalPauseArchive.writeTimelineCSV(
        "Number of reducer in pause state",
        this.t1,
        1000,
        { records =>
          val (timestamp, pauses) = records.foldLeft(
            (0.toLong, Map[String, Boolean]())
          ) {
            case ((_, ps), (t2p, (id, inPause))) =>
              val nps = if (ps contains id) {
                ps.updated(id, inPause)
              } else {
                ps + (id -> inPause)
              }

              pauseState.update(id, inPause)
              (t2p, nps)
          }
          val allPauseStates = pauseState.foldLeft(pauses) {
            case (ps, (id, inPause)) =>
              if (ps contains id) ps else ps + (id -> inPause)
          }
          val nbOfInactiveAgents = allPauseStates count {
            case (_, inPause) => inPause
          }

          (timestamp, nbOfInactiveAgents)
        },
        true,
        _.toString
      )
    }

    // ---------- IDLE STATE TIMELINE ---------- //

    val idleArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
          (fileName endsWith "idle.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val idleArchives = idleArchivesFileNames map {
      archiveFileName => new Archive[(String, Boolean), Int](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, (splitLine(1), splitLine(2).toBoolean))
        }
      )
    }

    if (!idleArchives.isEmpty) {
      val globalIdleArchive = idleArchives.tail.foldLeft(idleArchives.head) {
        case (acc, archive) => acc.fuseWith(archive, "idle_global")
      }
      val managerIds = this.reducers map {
        reducer => "manager@" + reducer.path.name
      }
      val idleState = {
        val m = mutable.Map[String, Boolean]()

        managerIds.foldLeft(m) { case (acc, id) => acc + (id -> false) }
      }

      globalIdleArchive.writeTimelineCSV(
        "Number of reducer in idle state",
        this.t1,
        1000,
        { records =>
          val (timestamp, idles) = records.foldLeft(
            (0.toLong, Map[String, Boolean]())
          ) {
            case ((_, is), (t2i, (id, isIdle))) =>
              val nis = if (is contains id) {
                is.updated(id, isIdle)
              } else {
                is + (id -> isIdle)
              }

              idleState.update(id, isIdle)
              (t2i, nis)
          }
          val allIdleStates = idleState.foldLeft(idles) {
            case (is, (id, isIdle)) =>
              if (is contains id) is else is + (id -> isIdle)
          }
          val nbOfIdleAgents = allIdleStates count {
            case (_, isIdle) => isIdle
          }

          (timestamp, nbOfIdleAgents)
        },
        true,
        _.toString
      )
    }

    // ---------- FREE STATE TIMELINE ---------- //

    val freeArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
          (fileName endsWith "free.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val freeArchives = freeArchivesFileNames map {
      archiveFileName => new Archive[(String, Boolean), Int](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, (splitLine(1), splitLine(2).toBoolean))
        }
      )
    }

    if (!freeArchives.isEmpty) {
      val globalFreeArchive = freeArchives.tail.foldLeft(freeArchives.head) {
        case (acc, archive) => acc.fuseWith(archive, "free_global")
      }
      val workerIds = this.reducers map {
        reducer => "worker@" + reducer.path.name
      }
      val freeState = {
        val m = mutable.Map[String, Boolean]()

        workerIds.foldLeft(m) { case (acc, id) => acc + (id -> false) }
      }

      globalFreeArchive.writeTimelineCSV(
        "Number of worker in free state",
        this.t1,
        1000,
        { records =>
          val (timestamp, frees) = records.foldLeft(
            (0.toLong, Map[String, Boolean]())
          ) {
            case ((_, fs), (t2f, (id, isFree))) =>
              val nfs = if (fs contains id) {
                fs.updated(id, isFree)
              } else {
                fs + (id -> isFree)
              }

              freeState.update(id, isFree)
              (t2f, nfs)
          }
          val allFreeStates = freeState.foldLeft(frees) {
            case (fs, (id, isFree)) =>
              if (fs contains id) fs else fs + (id -> isFree)
          }
          val nbOgFreeWorkers = allFreeStates count {
            case (_, isFree) => isFree
          }

          (timestamp, nbOgFreeWorkers)
        },
        true,
        _.toString
      )
    }

    // ---------- TASK SPLIT TIMELINE ---------- //

    val taskSplitArchivesFileNames = resultsFiles filter {
      fileName =>
        (fileName startsWith "historical") &&
        (fileName endsWith "splits.csv")
    } map {
      fileName => this.config.resultPath + fileName
    }
    val taskSplitArchives = taskSplitArchivesFileNames map {
      archiveFileName => new Archive[Int, Int](
        "",
        archiveFileName,
        true,
        { line =>
          val splitLine = line split ";"

          (splitLine(0).toLong, splitLine(1).toInt)
        }
      )
    }

    if (!taskSplitArchives.isEmpty) {
      val globalTaskSplitArchive =
        taskSplitArchives.tail.foldLeft(taskSplitArchives.head) {
          case (acc, archive) => acc.fuseWith(archive, "splits_global")
        }

      globalTaskSplitArchive.writeTimelineCSV(
        "Number of task split",
        this.t1,
        1000,
        { records => records.foldLeft((0.toLong, 0)) {
          case ((_, nbTaskSplit), (t, x)) => (t, nbTaskSplit + x)
        } },
        true,
        _.toString
      )
    }

    // ---------- OWNERSHIP RATE ---------- //

    val init: Map[Double, Int] =
      ((0.toDouble to 1.toDouble by 0.1).map(utils.roundUpDouble(_, 1)) map {
        _ -> 0
      }).toMap
    val allInitialTasksByMaxOwnershipRate =
      init :: reducersData.map(_.managerData.initialTasksByMaxOwnershipRate)
    val allInitialTasksByOwnerOwnershipRate =
      init :: reducersData.map(_.managerData.initialTasksByOwnerOwnershipRate)
    val allFinalTasksByOwnerOwnershipRate =
      init :: reducersData.map(_.foremanData)

    def mergeTaskBundleStates(maps: List[Map[Double, Int]]): Map[Double, Int] =
      maps.tail.foldLeft(allInitialTasksByMaxOwnershipRate.head) {
        case (acc, map) => map.foldLeft(acc) {
          case (acc2, (ownershipRate, nb)) =>
            val key = utils.roundUpDouble(ownershipRate, 1)
            if (acc2 contains key) {
              acc2.updated(key, acc2(key) + nb)
            } else {
              acc2 + (key -> nb)
            }
        }
      }

    val taskCountByMaxOwnershipRate =
      mergeTaskBundleStates(allInitialTasksByMaxOwnershipRate)
    val taskCountByOwnerOwnershipRate =
      mergeTaskBundleStates(allInitialTasksByOwnerOwnershipRate)
    val taskCountByFinalOwnerOwnershipRate =
      mergeTaskBundleStates(allFinalTasksByOwnerOwnershipRate)
    val maxOwnershipDataFile =
      new File(this.config.resultPath + "count_by_max_ownership.dat")
    val ownerOwnershipDataFile =
      new File(this.config.resultPath + "count_by_owner_ownership.dat")
    val finalOwnerOwnershipDataFile =
      new File(this.config.resultPath + "count_by_final_ownership.dat")
    val maxOwnershipPlotFile =
      new File(this.config.resultPath + "count_by_max_ownership.plot")
    val ownerOwnershipPlotFile =
      new File(this.config.resultPath + "count_by_owner_ownership.plot")
    val finalOwnershipPlotFile =
      new File(this.config.resultPath + "count_by_final_ownership.plot")

    // Write data files
    FileWriter.writeWith(
      maxOwnershipDataFile,
      (taskCountByMaxOwnershipRate.toList.sortBy(_._1) map {
        case (ownershipRate, nb) => ownershipRate + " " + nb + "\n"
      }).toIterator
    )
    FileWriter.writeWith(
      ownerOwnershipDataFile,
      (taskCountByOwnerOwnershipRate.toList.sortBy(_._1) map {
        case (ownershipRate, nb) => ownershipRate + " " + nb + "\n"
      }).toIterator
    )
    FileWriter.writeWith(
      finalOwnerOwnershipDataFile,
      (taskCountByFinalOwnerOwnershipRate.toList.sortBy(_._1) map {
        case (ownershipRate, nb) => ownershipRate + " " + nb + "\n"
      }).toIterator
    )

    // Write plot files
    FileWriter.writeWith(
      maxOwnershipPlotFile,
      Iterator(
        "set terminal '" + this.config.gnuplotOutputFormat + "'\n",
        "set output 'count_by_max_ownership." + this.config.gnuplotOutputFormat + "'\n",
        "set title \"Count by maximum ownership rate\n",
        "set boxwidth 0.75\n",
        "set style data boxes\n",
        "set style fill solid\n",
        "set yrange [0:*]\n",
        "plot for [COL=2:2] 'count_by_max_ownership.dat' using COL:xticlabels(1) title ''\n"
      )
    )
    FileWriter.writeWith(
      ownerOwnershipPlotFile,
      Iterator(
        "set terminal '" + this.config.gnuplotOutputFormat + "'\n",
        "set output 'count_by_owner_ownership." + this.config.gnuplotOutputFormat + "'\n",
        "set title \"Count by initial owner ownership rate\n",
        "set boxwidth 0.75\n",
        "set style data boxes\n",
        "set style fill solid\n",
        "set yrange [0:*]\n",
        "plot for [COL=2:2] 'count_by_owner_ownership.dat' using COL:xticlabels(1) title ''\n"
      )
    )
    FileWriter.writeWith(
      finalOwnershipPlotFile,
      Iterator(
        "set terminal '" + this.config.gnuplotOutputFormat + "'\n",
        "set output 'count_by_final_ownership." + this.config.gnuplotOutputFormat + "'\n",
        "set title \"Count by final owner ownership rate\n",
        "set boxwidth 0.75\n",
        "set style data boxes\n",
        "set style fill solid\n",
        "set yrange [0:*]\n",
        "plot for [COL=2:2] 'count_by_final_ownership.dat' using COL:xticlabels(1) title ''\n"
      )
    )

    // ---------- FAIRNESS BENEFIT ---------- //

//    val fairnessBenefitArchivesFileNames = resultsFiles filter {
//      fileName =>
//        (fileName startsWith "historical") &&
//        (fileName endsWith "fairness_benefit.csv")
//    } map {
//      fileName => this.config.resultPath + fileName
//    }
//    val fairnessBenefitArchives = fairnessBenefitArchivesFileNames map {
//      archiveFileName =>
//        new Archive[(Long, Long, Long, Double), Map[Double, Long]](
//          "",
//          archiveFileName,
//          true,
//          { line =>
//            val splitLine = line split ";"
//
//            (
//              splitLine(0).toLong,
//              (
//                splitLine(1).toLong,
//                splitLine(2).toLong,
//                splitLine(3).toLong,
//                splitLine(4).toDouble
//              )
//            )
//          }
//        )
//    }
//
//    if (!fairnessBenefitArchives.isEmpty) {
//      val globalFairnessBenefitArchive =
//        fairnessBenefitArchives.tail.foldLeft(fairnessBenefitArchives.head) {
//          case (acc, archive) =>
//            acc.fuseWith(archive, "fairness_benefit_global")
//        }
//      val steps = (1 to 10) map { x => x.toDouble / 10 }
//      val initialMap = (steps map { step => step -> 0.toLong }).toMap
//      val legend =
//        "[0.0,0.1];[0.1,0.2];[0.2,0.3];[0.3,0.4];[0.4,0.5];" +
//        "[0.5,0.6];[0.6,0.7];[0.7,0.8];[0.8,0.9];[0.9,1.0]"
//
//      globalFairnessBenefitArchive.writeTimelineCSV(
//        legend,
//        this.t1,
//        1000,
//        { records => records.foldLeft((0.toLong, initialMap)) {
//          case ((_, map), (t, (_, _, _, fairnessBenefit))) =>
//            val key = (steps find { _ >= fairnessBenefit }).get
//
//            (t, map.updated(key, map(key) + 1))
//        } },
//        true,
//        { map =>
//          val orderedKeys = map.keys.toList.sorted
//          val orderedValues = orderedKeys map { key => map(key) }
//
//          orderedValues mkString ";"
//        }
//      )
//    }

    // ---------- INITIALE TASK ALLOCATION ---------- //

    val workloadFilesNames = for (
      i <- 1 to this.config.nbReducer
    ) yield "historical_manager@reducer" + i + "_contribution.csv"
    val initialWorkloads =
      workloadFilesNames.foldLeft(Map[String, String]()) {
        case (acc, fileName) =>
          val sndLine = Source.fromFile(
            this.config.resultPath + fileName
          ).getLines().toList.tail.head
          val splitLine = sndLine.split(";")
          val workload = splitLine(2)
          val reducerNb = splitLine(1).split("reducer")(1)

          acc + (reducerNb -> workload)
      }
    val initialWorkloadsText =
      initialWorkloads.toList.sortWith {
        case ((r1, _), (r2, _)) => r1.toInt < r2.toInt
      }.foldRight(List[String]()) {
        case ((reducer, workload), acc) =>
          reducer + " " + workload + "\n" :: acc
      }
    val initialWorkloadFile = new File(
      this.config.resultPath + "initial_workloads.dat"
    )

    FileWriter.writeWith(initialWorkloadFile, initialWorkloadsText.toIterator)

    context stop self
  }

  /** @see akka.actor.Actor.receive() */
  def receive: Receive = {
    // No remote configuration to wait
    if (this.remoteRFH.isEmpty) {
      self ! ConfigOk
    }

    this.waitConfigDeployment(this.remoteRFH) orElse
    this.handleUnexpected("waitConfigDeployment")
  }

  /** State in which the monitor waits that the configuration is well
    * distributed among remote nodes.
    */
  def waitConfigDeployment(rfhsToWait: List[ActorRef]): Receive = {

    case ConfigOk if (rfhsToWait contains sender) || (sender == self) =>
      val newRfhsToWait = rfhsToWait diff List(sender)

      // The Configuration is well deployed
      if (newRfhsToWait.isEmpty) {
        // Create local and remote agents
        def createLocalAndRemoteAgents(
          totalNumberOfAgents: Int,
          remoteAgentsDescription: List[(String, Int)],
          localAgentCreationFunction: (Int, ActorRef) => ActorRef,
          remoteAgentCreationFunction: (Int, Address, ActorRef) => ActorRef
        ): List[ActorRef] = {
          val (remoteOnes, cpt) =
            remoteAgentsDescription.foldLeft((List[ActorRef](), 0)) {
              case ((remotes, innerCpt), (stringAddress, nb)) =>
                val address = AddressFromURIString(stringAddress)
                val remoteRFH = this.remoteAddressToRFH(stringAddress)
                val newRemotes = for {
                  i <- 1 to nb
                } yield remoteAgentCreationFunction(
                  innerCpt + i,
                  address,
                  remoteRFH
                )

                (remotes ++ newRemotes, innerCpt + nb)
            }
          val localOnes = for {
            i <- (cpt + 1) to totalNumberOfAgents
          } yield localAgentCreationFunction(i, this.localRFH)

          remoteOnes ++ localOnes
        }
        val (localReducerCreationFunction, remoteReducerCreationFunction) =
          if (this.config.taskMonitor) {
            (
              this.createLocalReducerWithMonitor _,
              this.createRemoteReducerWithMonitor _
            )
          } else {
            (
              this.createLocalReducer _,
              this.createRemoteReducer _
            )
          }

        // Launch mappers and reducers
        this.reducers = createLocalAndRemoteAgents(
          this.config.nbReducer,
          this.config.remoteReducers,
          localReducerCreationFunction,
          remoteReducerCreationFunction
        )
        this.mappers = createLocalAndRemoteAgents(
          this.config.nbMapper,
          this.config.remoteMappers,
          this.createLocalMapper,
          this.createRemoteMapper
        )
        this.partitioner = context.actorOf(
          Props(
            classOf[Partitioner],
            this.config.partitionStrategy,
            this.mappers.toSet,
            this.reducers
          ),
          name = "partitioner"
        )

        // Initiate task monitor
        if (this.config.taskMonitor) {
          // Creates TaskMonitor and adds it as a TaskEvent listener to each
          // adaptive reducer
          val taskMonitor = context.actorOf(
            Props(classOf[utils.taskMonitor.TaskMonitor]),
            name = "taskMonitor"
          )

          this.reducers foreach {
            _ ! utils.taskMonitor.TaskEventHandler.AddListener(taskMonitor)
          }
        }

        this.goToAskRFHToReducers(
          this.reducers.toSet,
          Map[ActorRef, ActorRef]()
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

  /** State in which the monitor ask its RFH to each reducer of the system. */
  def askRFHToReducers(
    reducersToWait: Set[ActorRef],
    rfhMap: Map[ActorRef, ActorRef],
    currentTimer: ActorRef
  ): Receive = {

    case GetRFH(rfh) if reducersToWait contains sender =>
      debugReceive("GetRFH", sender, "askRFHToReducers")
      val newRFHMap = rfhMap + (sender -> rfh)
      val newReducersToWait = reducersToWait - sender

      if (newReducersToWait.isEmpty) {
        this.goToBroadcastRFH(this.reducers.toSet + this.partitioner, newRFHMap)
      } else {
        context become this.askRFHToReducers(
          newReducersToWait,
          newRFHMap,
          currentTimer
        )
      }

    case Timer.Timeout if sender == currentTimer =>
      debugReceive("Timer.Timeout", sender, "askRFHToReducers")
      this.goToAskRFHToReducers(reducersToWait, rfhMap)

  }

  def broadcastRFH(
    agentsToWait: Set[ActorRef],
    rfhMap: Map[ActorRef, ActorRef],
    currentTimer: ActorRef
  ): Receive = {

    case BroadcastRFHOk if agentsToWait contains sender =>
      val newAgentsToWait = agentsToWait - sender

      if (newAgentsToWait.isEmpty) {
        this.goToWaitMappers(this.getMappersAndChunks)
      } else {
        context become this.broadcastRFH(
          agentsToWait - sender,
          rfhMap,
          currentTimer
        )
      }

    case Timer.Timeout if sender == currentTimer =>
      debugReceive("Timer.Timeout", sender, "broadcastRFH")
      this.goToBroadcastRFH(agentsToWait, rfhMap)

  }

  /** State in which the monitor waits for its mappers to do their task.
    *
    * @param mappersToWait mappers for which the monitor waits an
    *                      ExecuteOk message
    * @param timer         current timer
    */
  def waitMappers(
    mappersToWait: Map[ActorRef, List[Chunk]],
    timer: ActorRef
  ): Receive = {

    case ExecuteOk if mappersToWait contains sender =>
      debugReceive("ExecuteOk", sender, "waitMappers")

      val newMappersToWait = mappersToWait - sender

      if (newMappersToWait.isEmpty) {
        timer ! Timer.Cancel
      }

      context become (
        this.waitMappers(newMappersToWait, timer)
          orElse this.handleUnexpected("waitMappers")
      )

    case StartPartitioning =>
      debugReceive("StartPartitioning", sender, "waitMappers")
      // Start the reduce phase timer
      this.t1 = System.currentTimeMillis()
      timer ! Timer.Cancel
      // Kill the mappers
      this.mappers foreach {
        mapper =>
          debugSend("AdaptiveMapper.Kill", "waitPartitionerExecution")
          mapper ! AdaptiveMapper.Kill
      }
      context become (
        this.waitPartitionerExecution
          orElse this.handleDeprecatedExecuteOk("waitMappersExecution")
          orElse this.handleDeprecatedTimeout("waitMappersExecution")
          orElse this.handleUnexpected("waitMappersExecution")
      )

    case Timer.Timeout if sender == timer =>
      this.goToWaitMappers(mappersToWait)

  }

  /** State in which the monitor waits for the partitioner execution. */
  def waitPartitionerExecution: Receive = {

    case PartitionDone =>
      debugReceive("PartitionDone", sender, "waitPartitionerState")
      this.mappers foreach {
        mapper =>
          debugSend("AdaptiveMapper.Kill", "waitPartitionerExecution")
          mapper ! AdaptiveMapper.Kill
      }
      this goToWaitReducersState this.reducers.toSet

  }

  /** State in which the monitor waits for its reducers to be ready.
    *
    * @param reducersToWait number of reducers to wait
    * @param timer current timer
    */
  def waitReducersState(
    reducersToWait: Set[ActorRef],
    timer: ActorRef
  ): Receive = {

    case AcquaintancesOk =>
      val remainingReducers = reducersToWait - sender

      if (remainingReducers.isEmpty) {
        timer ! Timer.Cancel
        this.t1 = System.currentTimeMillis()
        this goToWaitReducersExecution this.reducers.toSet
      } else {
        context become (
          this.waitReducersState(remainingReducers, timer)
          orElse this.handleDeprecatedTimeout("waitReducersState")
          orElse this.handleUnexpected("waitReducersState")
        )
      }

    case Timer.Timeout if sender == timer =>
      this goToWaitReducersState reducersToWait

  }

  /** State in which the monitor wait the reducers to confirm their execution.
    *
    * @param reducersToWait reducers from which the monitor waits an ExecuteOk
    *                       message
    * @param timer current timer
    */
  def waitReducersExecution(
    reducersToWait: Set[ActorRef],
    timer: ActorRef
  ): Receive = {

    case ExecuteOk =>
      val newReducersToWait = reducersToWait - sender

      if (reducersToWait.isEmpty) {
        timer ! Timer.Cancel
        context become (
          this.active(
            this.reducers.toSet,
            this.reducers.map {
              r => this.getReducerNumberFromReducerName(r.path.name)
            },
            0
          )
          orElse this.handleDeprecatedExecuteOk("active")
          orElse this.handleDeprecatedTimeout("active")
          orElse this.handleUnexpected("active")
        )
      } else {
        context become (
          this.waitReducersExecution(newReducersToWait, timer)
          orElse this.active(
            this.reducers.toSet,
            this.reducers.map {
              r => this.getReducerNumberFromReducerName(r.path.name)
            },
            0
          )
          orElse this.handleDeprecatedAcquaintancesOk("waitReducers/active")
          orElse this.handleDeprecatedTimeout("waitReducers/active")
          orElse this.handleUnexpected("waitReducers/active")
        )
      }

    case Timer.Timeout if sender == timer =>
      this goToWaitReducersExecution reducersToWait

  }

  /** Active state of the monitor.
    *
    * @param activeReducers currently active reducers
    * @param activeList     list of active reducers - list contains reducer's
    *                       number
    */
  def active(
    activeReducers: Set[ActorRef],
    activeList: List[String],
    attemptToEndId: Int
  ): Receive = {

    case ReducerActive(reducerName) =>
      val newActiveReducers = activeReducers + sender
      val newActiveList =
        this.getReducerNumberFromReducerName(reducerName) :: activeList

      debugReceive(
        "ReducerActive "
        + reducerName
        + " - remaining = "
        + newActiveReducers.size
        + "="
        + newActiveList.mkString("(", ",", ")"),
        sender,
        "active"
      )
      print(
        "*** ReducerActive\t"
        + reducerName
        + " \t- remaining = "
        + newActiveReducers.size
        + "      \r"
      )
      context become (
        this.active(newActiveReducers, newActiveList, attemptToEndId)
        orElse this.handleDeprecatedExecuteOk("active")
        orElse this.handleDeprecatedTimeout("active")
        orElse this.handleDeprecatedReadyToDieAnswer(attemptToEndId, "active")
        orElse this.handleUnexpected("active")
      )
      debugSend("Manager.ReduceActiveOk", "active")
      sender ! Manager.ReducerActiveOk

    case ReducerIdle(reducerName, idleTime) =>
      val newActiveReducers = activeReducers - sender
      val newActiveList = activeList.filter {
        _ != this.getReducerNumberFromReducerName(reducerName)
      }

      this.reducerTimes.update(reducerName, idleTime - this.t1)

      debugReceive(
        "ReducerIdle "
        + reducerName
        + " - remaining = "
        + newActiveReducers.size
        + "="
        + newActiveList.mkString("(", ",", ")"),
        sender,
        "active"
      )
      debugSend("Manager.ReducerIdleOk", "active")
      sender ! Manager.ReducerIdleOk
      print(
        "*** ReducerIdle \t"
        + reducerName
        + " \t- remaining = "
        + newActiveReducers.size
        + "      \r"
      )

      if (newActiveReducers.isEmpty) {
        this.goToWaitKillConfirmation(attemptToEndId + 1, this.reducers.toSet)
        this.t2 = idleTime
      } else {
        context become (
          this.active(newActiveReducers, newActiveList, attemptToEndId)
          orElse this.handleDeprecatedExecuteOk("active")
          orElse this.handleDeprecatedTimeout("active")
          orElse this.handleDeprecatedReadyToDieAnswer(attemptToEndId, "active")
          orElse this.handleUnexpected("active")
        )
      }

  }

  def waitKillConfirmation(
    attemptToEndId: Int,
    reducersToWaitFor: Set[ActorRef],
    currentTimer: ActorRef
  ): Receive = {

    case ReducerActive(reducerName) =>
      val activeList =
        List(this.getReducerNumberFromReducerName(reducerName))

      debugReceive(
        "ReducerActive "
          + reducerName
          + " - remaining = "
          + "1"
          + "="
          + activeList.mkString("(", ",", ")"),
        sender,
        "waitKillConfirmation"
      )
      print(
        "*** ReducerActive\t"
          + reducerName
          + " \t- remaining = "
          + "1"
          + "      \r"
      )
      context become (
        this.active(Set(sender), activeList, attemptToEndId)
          orElse this.handleDeprecatedExecuteOk("active")
          orElse this.handleDeprecatedTimeout("active")
          orElse this.handleUnexpected("active")
        )
      debugSend("Manager.ReduceActiveOk", "active")
      sender ! Manager.ReducerActiveOk

    case ReadyToDieAnswer(id, false) if id == attemptToEndId =>
      debugReceive(
        "ReadyToDieAnswer(" + id + ", false)",
        sender,
        "waitKillConfirmation"
      )
      context become (
        this.active(Set(), List(), attemptToEndId)
        orElse this.handleDeprecatedExecuteOk("active")
        orElse this.handleDeprecatedTimeout("active")
        orElse this.handleUnexpected("active")
      )

    case ReadyToDieAnswer(id, true) if id == attemptToEndId =>
      debugReceive(
        "ReadyToDieAnswer(" + id + ", true)",
        sender,
        "waitKillConfirmation"
      )

      val newReducersToWaitFor = reducersToWaitFor - sender

      if (newReducersToWaitFor.nonEmpty) {
        context become (
          this.waitKillConfirmation(
            attemptToEndId,
            newReducersToWaitFor,
            currentTimer
          )
            orElse this.handleDeprecatedExecuteOk("waitKillConfirmation")
            orElse this.handleDeprecatedTimeout("waitKillConfirmation")
            orElse this.handleUnexpected("waitKillConfirmation")
          )
      } else {
        this.goToWaitProperKill(this.reducers.toSet, Set())
      }

    case Timer.Timeout if sender == currentTimer =>
      debugReceive("Timer.Timeout", sender, "waitKillConfirmation")
      this.goToWaitKillConfirmation(attemptToEndId, reducersToWaitFor)

  }

  /** State in which the monitor wait its reducers to stop.
    *
    * @param timer          current timer
    * @param reducersToWait reducers from which the monitor waits a KillOk
    *                       message
    * @param reducersData   data produced by the reducers during the run
    */
  def waitProperKill(
    timer: ActorRef,
    reducersToWait: Set[ActorRef],
    reducersData: Set[ReducerData]
  ): Receive = {

    case KillOk(reducerData) =>
      debugReceive("KillOk", sender, "waitProperKill")

      val newReducersToWait = reducersToWait - sender
      val newReducersData = reducersData + reducerData

      if (newReducersToWait.isEmpty) {
        timer ! Timer.Cancel

        if (this.remoteRFH.isEmpty) {
          this.kill(newReducersData.toList)
        } else {
          this.localRFH ! RemoteFileHandler.GetResFiles(this.remoteRFH)
          context become (
            this.waitRepatriation(
              this.remoteRFH.length,
              0,
              newReducersData.toList
            )
            orElse this.handleUnexpected("waitRepatriation")
          )
        }
      } else {
        context become this.waitProperKill(
          timer,
          newReducersToWait,
          newReducersData
        )
      }

    case Timer.Timeout if sender == timer =>
      this goToWaitProperKill(reducersToWait, reducersData)

  }

  /** State in which the monitor waits for its RFH to repatriate the results
    * files.
    */
  def waitRepatriation(
    rfhMessagesToWait: Int,
    killedRFH: Int,
    reducersData: List[ReducerData]
  ): Receive = {

    case RemoteFileHandler.FilesAvailable if rfhMessagesToWait == 1 =>
      this.remoteRFH foreach { _ ! RemoteFileHandler.RemoveResults }

    case RemoteFileHandler.FilesAvailable =>
      context become (
        this.waitRepatriation(rfhMessagesToWait - 1, killedRFH, reducersData)
        orElse this.handleUnexpected("waitRepatriation")
      )

    case ResultsRemoved if killedRFH + 1 == this.remoteRFH.length =>
      this.kill(reducersData)

    case ResultsRemoved =>
      context become (
        this.waitRepatriation(rfhMessagesToWait, killedRFH + 1, reducersData)
        orElse this.handleUnexpected("waitRepatriation")
      )

  }

  /** Handle deprecated AcquaintancesOk messages.
    *
    * @param state state in which the deprecated AcquaintancesOk message is
    *              received
    */
  def handleDeprecatedAcquaintancesOk(state: String): Receive = {

    case AcquaintancesOk =>
      debugDeprecated("AcquaintancesOk", sender, state)

  }

  /** Handle deprecated GetRFH messages.
    *
    * @param state state in which the deprecated GetRFH message is received
    */
  def handleDeprecatedGetRFH(state: String): Receive = {

    case GetRFH(_) =>
      debugDeprecated("GetRFH", sender, state)

  }

  /** Handle deprecated BroadcastOk messages.
    *
    * @param state state in which the deprecated BroadcastOk message is received
    */
  def handleDeprecatedRFHBroadcastOk(state: String): Receive = {

    case BroadcastRFHOk =>
      debugDeprecated("BroadcastOk", sender, state)

  }

  /** Handle deprecated ExecuteOk messages.
    *
    * @param state state in which the deprecated ExecuteOk message is received
    */
  def handleDeprecatedExecuteOk(state: String): Receive = {

    case ExecuteOk =>
      debugDeprecated("ExecuteOk", sender, state)

  }

  /** Handle the deprecated Timeout messages.
    *
    * @param state state in which the deprecated Timeout message is received
    */
  def handleDeprecatedTimeout(state: String): Receive = {

    case Timer.Timeout =>
      debugDeprecated("Timer.Timeout", sender, state)

  }

  /** Handle the deprecated ReducerIdle messages.
    *
    * @param state state in which the deprecated ReducerIdle message is received
    */
  def handleDeprecatedReducerIdle(state: String): Receive = {

    case ReducerIdle =>
      debugDeprecated("ReducerIdle", sender, state)
      // If the sender does not receive the first ReducerIdleOk, ...
      debugSend("Manager.ReducerIdleOk", state)
      sender ! Manager.ReducerIdleOk
      // we can suppose it has not receive the following Kill message either
      debugSend("AdaptiveReducer.Kill", state)
      sender ! AdaptiveReducer.Kill

  }

  /** Handle the deprecated ReadyToDieAnswer messages.
    *
    * @param attemptToEndId identifier of the attempt to end the run
    * @param state          state in which the deprecated ReadyToDieAnswer
    *                       message is received
    */
  def handleDeprecatedReadyToDieAnswer(
    attemptToEndId: Int,
    state: String
  ): Receive = {

    case ReadyToDieAnswer(id, ready) if id < attemptToEndId =>
      debugDeprecated(
        "ReadyToDieAnswer(" + id + ", " + ready + ")",
        sender,
        state
      )

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

  /** Post stop actions.
    * 1. Print useful informations (time, fairness, etc.)
    * 2. Create the gnuplot script to generate the theoretical fairness plot
    */
  override def postStop: Unit = {

    // ---------- PRINTS ---------- //

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

    // ---------- GNUPLOT NB VALUES SCRIPT ---------- //

    val contributionFile =
      new File(this.config.resultPath + "contribution.plot")
    val cbw = new BufferedWriter(new java.io.FileWriter(contributionFile))
    val n = this.reducers.length
    val contributionTitle = this.config.gnuplotTitle + "\\n"
      "job : " + this.config.jobName + "\\n"
      "pause = " + this.config.milliPause + "ms" +
      this.config.nanoPause + "ns"

    cbw.write("set terminal '" + this.config.gnuplotOutputFormat + "'\n")
    cbw.write(
      "set output '" + this.config.gnuplotOutputFilename + "." +
        this.config.gnuplotOutputFormat + "'\n"
    )
    cbw.write("set title \"" + contributionTitle + "\"\n")
    cbw.write("set boxwidth 0.75\n")

    cbw.write("set style data histograms\n")
    cbw.write("set style histogram rowstacked\n")

    cbw.write("set style fill solid\n")
    cbw.write("set yrange [0:*]\n")

    cbw.write("system('cat ")
    for (i <- 1 to n) {
      cbw.write(reducerNamePrefix + i + "_nbValues.dat ")
    }
    cbw.write("> all_nbValues.data')\n")
    cbw.write("system('cat worker@*.dat > results')\n")
    cbw.write("system('gnuplot cfp_timeline.plot')\n")
    cbw.write("system('gnuplot fairness_timeline.plot')\n")
    cbw.write("system('gnuplot pause_timeline.plot')\n")
    cbw.write("system('gnuplot idle_timeline.plot')\n")
    cbw.write("system('gnuplot min_max_timeline.plot')\n")
    cbw.write("system('gnuplot free_timeline.plot')\n")
    cbw.write("system('gnuplot delegation_costs_timeline.plot')\n")
    cbw.write("system('gnuplot nbValues_timeline.plot')\n")
    cbw.write("system('gnuplot workload_cost.plot')\n")
    cbw.write("system('gnuplot count_by_max_ownership.plot')\n")
    cbw.write("system('gnuplot count_by_owner_ownership.plot')\n")
    cbw.write("system('gnuplot count_by_final_ownership.plot')\n")
    cbw.write("system('gnuplot initial_workload_cost.plot')\n")
    if (this.config.withTaskSplit) {
      cbw.write("system('gnuplot splits_timeline.plot')\n")
    }

    cbw.write(
      "plot for [COL=3:" + (this.config.gnuplotMaxTaskDoneNumber + 3) +
        "] 'all_nbValues.data' using COL:xticlabels(1) title ''\n"
    )

    cbw.close()

    // ---------- GNUPLOT NB VALUES SCRIPT SIMPLIFIED ---------- //

    val simplifiedContributionFile =
      new File(this.config.resultPath + "simple_contribution.plot")
    val scbw = new BufferedWriter(
      new java.io.FileWriter(simplifiedContributionFile)
    )

    scbw.write("set terminal '" + this.config.gnuplotOutputFormat + "'\n")
    scbw.write(
      "set output 'simple_" + this.config.gnuplotOutputFilename + "." +
        this.config.gnuplotOutputFormat + "'\n"
    )
    scbw.write("set title \"" + "simple " + contributionTitle + "\"\n")
    scbw.write("set boxwidth 0.75\n")

    scbw.write("set style data boxes\n")

    scbw.write("set style fill solid\n")
    scbw.write("set yrange [0:*]\n")

    scbw.write("system('cat ")
    for (i <- 1 to n) {
      scbw.write(reducerNamePrefix + i + "_nbValues.dat ")
    }
    scbw.write("> all_nbValues.data')\n")
    scbw.write("system('cat worker@*.dat > results')\n")
    scbw.write("system('gnuplot cfp_timeline.plot')\n")
    scbw.write("system('gnuplot fairness_timeline.plot')\n")
    scbw.write("system('gnuplot pause_timeline.plot')\n")
    scbw.write("system('gnuplot idle_timeline.plot')\n")
    scbw.write("system('gnuplot min_max_timeline.plot')\n")
    scbw.write("system('gnuplot free_timeline.plot')\n")
    scbw.write("system('gnuplot delegation_costs_timeline.plot')\n")
    scbw.write("system('gnuplot nbValues_timeline.plot')\n")
    scbw.write("system('gnuplot simple_workload_cost.plot')\n")
    scbw.write("system('gnuplot count_by_max_ownership.plot')\n")
    scbw.write("system('gnuplot count_by_owner_ownership.plot')\n")
    scbw.write("system('gnuplot count_by_final_ownership.plot')\n")
    scbw.write("system('gnuplot initial_workload_cost.plot')\n")
    if (this.config.withTaskSplit) {
      scbw.write("system('gnuplot splits_timeline.plot')\n")
    }

    scbw.write(
      "plot for [COL=2:2] 'all_nbValues.data' using COL:xticlabels(1) title ''\n"
    )

    scbw.close()

    // ---------- GNUPLOT WORKLOAD SCRIPT ---------- //

    val workloadFile = new File(this.config.resultPath + "workload_cost.plot")
    val workloadBufferWriter =
      new BufferedWriter(new java.io.FileWriter(workloadFile))

    workloadBufferWriter.write(
      "set terminal '" + this.config.gnuplotOutputFormat + "'\n"
    )
    workloadBufferWriter.write(
      "set output 'workload_cost." + this.config.gnuplotOutputFormat + "'\n"
    )
    workloadBufferWriter.write(
      "set title \"" + "workload_cost\"\n"
    )
    workloadBufferWriter.write("set boxwidth 0.75\n")
    workloadBufferWriter.write("set style data boxes\n")
    workloadBufferWriter.write("set style fill solid\n")
    workloadBufferWriter.write("set yrange [0:*]\n")

    workloadBufferWriter.write("system('cat ")
    for (i <- 1 to n) {
      workloadBufferWriter.write(reducerNamePrefix + i + "_costs.dat ")
    }
    workloadBufferWriter.write("> all_costs.data')\n")
    workloadBufferWriter.write(
      "plot for [COL=3:" + (this.config.gnuplotMaxTaskDoneNumber + 3) +
        "] 'all_costs.data' using COL:xticlabels(1) title ''\n"
    )
    workloadBufferWriter.close()

    // ---------- GNUPLOT SIMPLIFIED WORKLOAD SCRIPT ---------- //

    val simpleWorkloadFile = new File(this.config.resultPath + "simple_workload_cost.plot")
    val simpleWorkloadBufferWriter =
      new BufferedWriter(new java.io.FileWriter(simpleWorkloadFile))

    simpleWorkloadBufferWriter.write(
      "set terminal '" + this.config.gnuplotOutputFormat + "'\n"
    )
    simpleWorkloadBufferWriter.write(
      "set output 'simple_workload_cost." + this.config.gnuplotOutputFormat + "'\n"
    )
    simpleWorkloadBufferWriter.write(
      "set title \"" + "simple workload cost\"\n"
    )
    simpleWorkloadBufferWriter.write("set boxwidth 0.75\n")
    simpleWorkloadBufferWriter.write("set style data boxes\n")
    simpleWorkloadBufferWriter.write("set style fill solid\n")
    simpleWorkloadBufferWriter.write("set yrange [0:*]\n")

    simpleWorkloadBufferWriter.write("system('cat ")
    for (i <- 1 to n) {
      simpleWorkloadBufferWriter.write(reducerNamePrefix + i + "_costs.dat ")
    }
    simpleWorkloadBufferWriter.write("> all_costs.data')\n")
    simpleWorkloadBufferWriter.write(
      "plot for [COL=2:2] 'all_costs.data' using COL:xticlabels(1) title ''\n"
    )
    simpleWorkloadBufferWriter.close()

    // ---------- GNUPLOT INITIAL WORKLOADS SCRIPT ---------- //

    val initialWorkloadFile = new File(this.config.resultPath + "initial_workload_cost.plot")
    val initialWorkloadBufferWriter =
      new BufferedWriter(new java.io.FileWriter(initialWorkloadFile))

    initialWorkloadBufferWriter.write(
      "set terminal '" + this.config.gnuplotOutputFormat + "'\n"
    )
    initialWorkloadBufferWriter.write(
      "set output 'initial_workload_cost." + this.config.gnuplotOutputFormat + "'\n"
    )
    initialWorkloadBufferWriter.write(
      "set title \"" + "initial workloads cost\"\n"
    )
    initialWorkloadBufferWriter.write("set boxwidth 0.75\n")
    initialWorkloadBufferWriter.write("set style data boxes\n")
    initialWorkloadBufferWriter.write("set style fill solid\n")
    initialWorkloadBufferWriter.write("set yrange [0:*]\n")

    initialWorkloadBufferWriter.write(
      "plot for [COL=2:2] 'initial_workloads.dat' using COL:xticlabels(1) title ''\n"
    )
    initialWorkloadBufferWriter.close()

    // ---------- GNUPLOT CFP TIMELINE SCRIPT ---------- //

    val cfpTimelineFile = new File(this.config.resultPath + "cfp_timeline.plot")
    val cfpTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(cfpTimelineFile))
    val cfpCsvFileName = "'timeline_CFP_global.csv'"

    cfpTimelineBufferWriter.write("set terminal 'png'\n")
    cfpTimelineBufferWriter.write("set datafile separator \";\"\n")
    cfpTimelineBufferWriter.write("set output 'cfp_timeline.png'\n")
    cfpTimelineBufferWriter.write("set key autotitle columnhead\n")
    cfpTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    cfpTimelineBufferWriter.write(
      "plot " + cfpCsvFileName + " u 0:2 w l lc rgb 'red', " + cfpCsvFileName +
        " u 0:3 w l lc rgb 'orangered', " + cfpCsvFileName +
        " u 0:4 w l lc rgb 'steelblue'"
    )

    cfpTimelineBufferWriter.close()

    // ---------- GNUPLOT FAIRNESS TIMELINE SCRIPT ----------- //

    val fairnessTimelineFile =
      new File(this.config.resultPath + "fairness_timeline.plot")
    val fairnessTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(fairnessTimelineFile))
    val fairnessCsvFileName = "'timeline_fairness_global.csv'"

    fairnessTimelineBufferWriter.write("set terminal 'png'\n")
    fairnessTimelineBufferWriter.write("set datafile separator \";\"\n")
    fairnessTimelineBufferWriter.write("set output 'fairness_timeline.png'\n")
    fairnessTimelineBufferWriter.write("set key autotitle columnhead\n")
    fairnessTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    fairnessTimelineBufferWriter.write(
      "plot " + fairnessCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    fairnessTimelineBufferWriter.close()

    // ---------- GNUPLOT COSTS FAIRNESS TIMELINE SCRIPT ---------- //

    val nbValuesTimelineFile =
      new File(this.config.resultPath + "nbValues_timeline.plot")
    val nbValuesTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(nbValuesTimelineFile))
    val nbValuesCsvFileName = "'timeline_nbValues_global.csv'"

    nbValuesTimelineBufferWriter.write("set terminal 'png'\n")
    nbValuesTimelineBufferWriter.write("set datafile separator \";\"\n")
    nbValuesTimelineBufferWriter.write("set output 'nbValues_timeline.png'\n")
    nbValuesTimelineBufferWriter.write("set key autotitle columnhead\n")
    nbValuesTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    nbValuesTimelineBufferWriter.write(
      "plot " + nbValuesCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    nbValuesTimelineBufferWriter.close()

    // ---------- GNUPLOT DELEGATION COSTS SCRIPT ----------- //

    val delegationCostsTimelineFile =
      new File(this.config.resultPath + "delegation_costs_timeline.plot")
    val delegationCostsTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(delegationCostsTimelineFile))
    val delegationCostsCsvFileName = "'timeline_delegation_costs_global.csv'"

    delegationCostsTimelineBufferWriter.write("set terminal 'png'\n")
    delegationCostsTimelineBufferWriter.write("set datafile separator \";\"\n")
    delegationCostsTimelineBufferWriter.write(
      "set output 'delegation_costs_timeline.png'\n"
    )
    delegationCostsTimelineBufferWriter.write("set key autotitle columnhead\n")
    delegationCostsTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    delegationCostsTimelineBufferWriter.write(
      "plot " + delegationCostsCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    delegationCostsTimelineBufferWriter.close()

    // ---------- GNUPLOT MIN MAX CONTRIB. SCRIPT ----------- //

    val minMaxTimelineFile =
      new File(this.config.resultPath + "min_max_timeline.plot")
    val minMaxTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(minMaxTimelineFile))
    val minMaxCSVFileName = "'timeline_fairness_global.csv'"

    minMaxTimelineBufferWriter.write("set terminal 'png'\n")
    minMaxTimelineBufferWriter.write("set datafile separator \";\"\n")
    minMaxTimelineBufferWriter.write("set output 'min_max_timeline.png'\n")
    minMaxTimelineBufferWriter.write("set key autotitle columnhead\n")
    minMaxTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    minMaxTimelineBufferWriter.write(
      "plot " + minMaxCSVFileName + " u 0:3 w l lc rgb 'steelblue', " +
        minMaxCSVFileName + " u 0:4 w l lc rgb 'orangered', " +
        minMaxCSVFileName + " u 0:5 w l lc rgb 'red'"
    )

    minMaxTimelineBufferWriter.close()

    // ---------- GNUPLOT PAUSE TIMELINE SCRIPT ----------- //

    val pauseTimelineFile =
      new File(this.config.resultPath + "pause_timeline.plot")
    val pauseTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(pauseTimelineFile))
    val pauseCsvFileName = "'timeline_pause_global.csv'"

    pauseTimelineBufferWriter.write("set terminal 'png'\n")
    pauseTimelineBufferWriter.write("set datafile separator \";\"\n")
    pauseTimelineBufferWriter.write("set output 'pause_timeline.png'\n")
    pauseTimelineBufferWriter.write("set key autotitle columnhead\n")
    pauseTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    pauseTimelineBufferWriter.write(
      "plot " + pauseCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    pauseTimelineBufferWriter.close()

    // ---------- GNUPLOT IDLE TIMELINE SCRIPT ----------- //

    val idleTimelineFile =
      new File(this.config.resultPath + "idle_timeline.plot")
    val idleTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(idleTimelineFile))
    val idleCsvFileName = "'timeline_idle_global.csv'"

    idleTimelineBufferWriter.write("set terminal 'png'\n")
    idleTimelineBufferWriter.write("set datafile separator \";\"\n")
    idleTimelineBufferWriter.write("set output 'idle_timeline.png'\n")
    idleTimelineBufferWriter.write("set key autotitle columnhead\n")
    idleTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    idleTimelineBufferWriter.write(
      "plot " + idleCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    idleTimelineBufferWriter.close()

    // ---------- GNUPLOT FREE TIMELINE SCRIPT ----------- //

    val freeTimelineFile =
      new File(this.config.resultPath + "free_timeline.plot")
    val freeTimelineBufferWriter =
      new BufferedWriter(new java.io.FileWriter(freeTimelineFile))
    val freeCsvFileName = "'timeline_free_global.csv'"

    freeTimelineBufferWriter.write("set terminal 'png'\n")
    freeTimelineBufferWriter.write("set datafile separator \";\"\n")
    freeTimelineBufferWriter.write("set output 'free_timeline.png'\n")
    freeTimelineBufferWriter.write("set key autotitle columnhead\n")
    freeTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
    freeTimelineBufferWriter.write(
      "plot " + freeCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
    )

    freeTimelineBufferWriter.close()

    // ----------- GNUPLOT TASK SPLIT TIMELINE SCRIPT ---------- //

    if (this.config.withTaskSplit) {
      val taskSplitTimelineFile =
        new File(this.config.resultPath + "splits_timeline.plot")
      val taskSplitTimelineBufferWriter =
        new BufferedWriter(new java.io.FileWriter(taskSplitTimelineFile))
      val taskSplitCsvFileName = "'timeline_splits_global.csv'"

      taskSplitTimelineBufferWriter.write("set terminal 'png'\n")
      taskSplitTimelineBufferWriter.write("set datafile separator \";\"\n")
      taskSplitTimelineBufferWriter.write("set output 'splits_timeline.png'\n")
      taskSplitTimelineBufferWriter.write("set key autotitle columnhead\n")
      taskSplitTimelineBufferWriter.write("set xlabel \"time (s)\"\n")
      taskSplitTimelineBufferWriter.write(
        "plot " + taskSplitCsvFileName + " u 0:2 w l lc rgb 'steelblue'"
      )

      taskSplitTimelineBufferWriter.close()
    }

    // ---------- GNUPLOT FAIRNESS BENEFIT TIMELINE SCRIPT ---------- //

    val fairnessBenefitTimelineFile =
      new File(this.config.resultPath + "fairness_benefit_plot.plot")
    val fairnessBenefitBufferWriter =
      new BufferedWriter(new java.io.FileWriter(fairnessBenefitTimelineFile))
    val fairnessBenefitCsvFileName = "'timeline_fairness_benefit_global.csv'"

    fairnessBenefitBufferWriter.write("set terminal 'png'\n")
    fairnessBenefitBufferWriter.write("set datafile separator \";\"\n")
    fairnessBenefitBufferWriter.write(
      "set output 'fairness_benefit_timeline.png'\n"
    )
    fairnessBenefitBufferWriter.write("set key autotitle columnhead\n")
    fairnessBenefitBufferWriter.write("set xlabel \"time (s)\"\n")
    fairnessBenefitBufferWriter.write("set style data histogram\n")
    fairnessBenefitBufferWriter.write("set style histogram cluster gap 1\n")
    fairnessBenefitBufferWriter.write("set style fill solid border -1\n")
    fairnessBenefitBufferWriter.write("set boxwidth 0.9\n")
    fairnessBenefitBufferWriter.write(
      "plot " + fairnessBenefitCsvFileName +
        " u 2:xtic(1) with histogram, '' u 3 w histogram, '' u 4 w histogram," +
        " '' u 5 w histogram, '' u 6 w histogram, '' u 7 w histogram, " +
        "'' u 8 w histogram, '' u 9 w histogram, '' u 10 w histogram, " +
        "'' u 11 w histogram"
    )

    fairnessBenefitBufferWriter.close()

    debug("****** FINISHED ")
  }

}
