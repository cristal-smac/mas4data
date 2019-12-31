package utils.config

import java.io.{File, FileInputStream, ObjectInputStream}

import akka.actor.ActorRef
import utils.bundles.TaskBundle
import utils.jobs.ReduceJob
import utils.strategies.partition.{LPTPartitionStrategy, PartitionStrategy}
import utils.strategies.taskCost.TaskCostStrategy
import utils.tasks.Task

import scala.io.Source

/** Build a Configuration object from a configuration text file or from a
  * serialized Configuration object.
  */
object ConfigurationBuilder {

  /** Folder path in which find the configuration text file. */
  val configFolder = "./config/"

  // Current configuration object
  private var currentConfig: Option[Configuration] = None

  // File which contains the serialized configuration object
  private val serializedConfigFile: File =
    new File(this.configFolder + "serializedConfig.dat")

  // File which contains the scratch configuration file path
  private val scratchConfigFile: File =
    new File(this.configFolder + "configLocation.txt")

  // Time where the scratch configuration file was last modified
  private var lastModifScratch: Long = _

  // Time where the serialized configuration file was last modified
  private var lastModifSerialized: Long = _

  /** Name of the serialized Configuration object. */
  val configSerializedConfigFileName = "serializedConfig.dat"

  // Build a Configuration object from a configuration text file.
  private def buildConfigFromScratch: Configuration = {
    val source = Source fromFile this.scratchConfigFile
    val configFilePath = source.getLines.next
    val fields = ConfigurationHandler getConfigFrom configFilePath

    source.close
    new Configuration(fields)
  }

  // Build a Configuration object from a serialized Configuration object.
  private def buildConfigFromSerialized: Configuration = {
    val is = new ObjectInputStream(
      new FileInputStream(this.serializedConfigFile)
    )
    val config = is.readObject

    is.close()
    config.asInstanceOf[Configuration]
  }

  /** Get the Configuration object depending on which resources is available
    * (text file or serialized Configuration object).
    */
  def config: Configuration = {
    // The current configuration object is initialized or updated if needed

    // The last modification time of the file which are currently on the disk
    val scratchLastModified = this.scratchConfigFile.lastModified
    val serialLastModified = this.serializedConfigFile.lastModified

    // There is a saved configuration object
    if (this.currentConfig.isDefined) {
      // There is no serialized configuration file, let's look at the scratch
      // configuration file
      if (serialLastModified == 0) {
        // The scratch configuration file has changed since the last save
        if (scratchLastModified != this.lastModifScratch) {
          // The saved configuration object is updated
          this.currentConfig = Some(this.buildConfigFromScratch)
          this.lastModifScratch = scratchLastModified
        }
      }
      // There is a serialized configuration file
      else {
        // The saved config object is not up to date
        if (serialLastModified != this.lastModifSerialized) {
          // The saved configuration object is updated
          this.currentConfig = Some(this.buildConfigFromSerialized)
          this.lastModifSerialized = serialLastModified
        }
      }
    }
    // There is no save configuration object
    else {
      // There is no serialized configuration file
      if (serialLastModified == 0) {
        // The saved configuration object is built from scratch
        this.currentConfig = Some(this.buildConfigFromScratch)
        this.lastModifScratch = scratchLastModified
      }
      // There is a serialized configuration file
      else {
        // The saved configuration object is built from the serialized file
        this.currentConfig = Some(this.buildConfigFromSerialized)
        this.lastModifSerialized = serialLastModified
      }
    }

    // The current configuration object is returned
    this.currentConfig.get
  }

}

/** Contains the configuration of the map reduce system. */
class Configuration(fields: Map[String, String]) extends Serializable {

  // ----- GLOBAL CONFIGURATION ----- //

  // Number of mappers
  lazy val nbMapper: Int = (this fields "nb-mapper").toInt

  // Number of reducers
  lazy val nbReducer: Int = (this fields "nb-reducer").toInt

  // Job name
  lazy val jobName: String = this fields "pb"

  // Job for the run : map and reduce jobs
  private lazy val jobs = JobHandler getJobsFrom this.jobName

  // Map job
  lazy val mapJob: Class[_] = this.jobs._1

  // Reduce job
  lazy val reduceJob: Class[_] = this.jobs._2

  // Maximum number of simultaneous auctions bidders can handle
  lazy val bidderMaxAuction: Option[Int] = {
    val value = this fields "bidder-max-auction"

    if (value == "none") None else Some(value.toInt)
  }

  // Threshold take into account in the negotiation process
  lazy val threshold: Double = try {
    (this fields "threshold").toDouble
  } catch {
    case _: NumberFormatException => 0.0
  }

  // Task bundle management strategy
  def taskBundle(
    initialTasks: List[Task],
    owner: ActorRef,
    rfhMap: Map[ActorRef, ActorRef]
  ): TaskBundle = TaskBundleHandler.getTaskBundleFrom(
    this fields "task-bundle-management-strategy",
    initialTasks,
    owner,
    rfhMap,
    this.threshold
  )


  // Initial data file path
  lazy val initFile: String = this fields "init-file"

  // Result directory path
  lazy val resultPath: String = this fields "result-path"

  // Determine the map chunks already exist
  lazy val initChunks: Boolean = (this fields "init-chunks").toBoolean

  // Determine the reduce chunks size
  lazy val chunkSize: Int = (this fields "chunk-size").toInt

  // Map chunks location
  lazy val initChunksPath: Option[String] =
    if (this.initChunks) Some(this fields "init-chunks-path") else None

  // Number of initial chunks
  lazy val initChunksNumber: Option[Int] =
    if (this.initChunks) {
      Some((this fields "init-chunks-number").toInt)
    } else {
      None
    }

  // Seed to manage the randomness of mappers local chunks multiplication
  lazy val mapperSeeds: Option[List[Int]] =
    if (this.fields contains "init-mapper-seed") {
      val initSeed = (this fields "init-mapper-seed").toInt
      val seeds = initSeed until (initSeed + this.nbMapper)

      Some(seeds.toList)
    } else None

  // Do mappers multiply their local chunks ?
  lazy val chunkMult: Boolean = (this fields "chunk-mult").toBoolean

  // base multiplier apply to every chunk of chunkMult
  lazy val chunkBaseMult: Int = (this fields "chunk-base-mult").toInt
  
  // Determine the range of chunks which have to be multiplied by a mapper
  lazy val chunkMultShift: Option[(Int, Int)] = if (this.chunkMult) {
    val stringBoundaries = (this fields "chunk-mult-shift") filter {
      case char => char.isDigit || (char == ' ')
    }
    val boundaries = stringBoundaries.split("\\s+")
    val (b1, b2) = (boundaries(0).toInt, boundaries(1).toInt)

    Some((scala.math.min(b1, b2), scala.math.max(b1, b2)))
  } else None

  // Get the shift list for a given mapper
  def getMultShiftForMapper(mapperNb: Int): List[Int] = {
    val (shift1, shift2) = this.chunkMultShift.get
    val shiftList = shift1 until (shift2 + shift1)

    (shiftList.map {
      case n =>
        val m = (n + mapperNb) % this.nbMapper
        if (m == 0) this.nbMapper else m
    }).toList.distinct
  }

  // Determine how many times mapper local chunks are multiplied
  lazy val nbMultChunks: Option[(Int, Int)] = if (this.chunkMult) {
    val stringBoundaries = (this fields "chunk-mult-nb") filter {
      case char => char.isDigit || (char == ' ')
    }
    val boundaries = stringBoundaries.split("\\s+")
    val (b1, b2) = (boundaries(0).toInt, boundaries(1).toInt)

    Some((scala.math.min(b1, b2), scala.math.max(b1, b2)))
  } else None

  // Frequency at which the manager must take into account an InformContribution
  // message
  lazy val informContributionFrequency: Int = try {
    this.fields("inform-contribution-frequency").toInt
  } catch {
    case _: Exception => 1
  }

  // Determine if a run is remote or not
  lazy val isRemoteRun: Boolean = (this fields "remote").toBoolean

  // Path of the file which contains the remote mappers partition
  lazy val remoteMappersLocation: String = this fields "remote-mappers"

  // Path of the file which contains the remote reducers partition
  lazy val remoteReducersLocation: String = this fields "remote-reducers"

  // Remote mappers
  lazy val remoteMappers: List[(String, Int)] =
    if (this.isRemoteRun) {
      RemoteAddressesHandler.getRemoteAddresses(this.remoteMappersLocation)
    } else {
      Nil
    }

  // Remote reducers
  lazy val remoteReducers: List[(String, Int)] =
    if (this.isRemoteRun) {
      RemoteAddressesHandler.getRemoteAddresses(this.remoteReducersLocation)
    } else {
      Nil
    }

  // Determine if the reducers are allowed to split tasks
  lazy val withTaskSplit: Boolean = (this fields "task-split").toBoolean

  // Partition strategy
  lazy val partitionStrategy: PartitionStrategy = {
    val reduceJobInstance = this.reduceJob.getDeclaredConstructor().newInstance() match {
      case job: ReduceJob => Some(job)
      case _              => None
    }

    if (reduceJobInstance.isDefined) {
      PartitionStrategyHandler.buildPartitionStrategy(
        this fields "partition-strategy",
        reduceJobInstance.get
      )
    } else {
      LPTPartitionStrategy
    }
  }

  // Task cost strategy
  lazy val taskCostStrategy: TaskCostStrategy =
    TaskCostStrategyHandler.getTaskCostStrategyFrom(
      this fields "task-cost-strategy"
    )

  // Worker pause in milli seconds
  lazy val milliPause: Long = (this fields "pause-millis").toLong

  // Worker pause in nano seconds
  lazy val nanoPause: Int = (this fields "pause-nanos").toInt

  // Timeouts in milliseconds
  lazy val timeouts: Map[String, Int] = {
    val timeoutRelative = this.fields filter {
      case (field, _) => field endsWith "timeout"
    }

    timeoutRelative map { case (field, value) => field -> value.toInt }
  }

  // Debugs
  lazy val debugs: Map[String, Boolean] = {
    val debugRelative = this.fields filter {
      case (field, _) => field startsWith "debug"
    }

    debugRelative map { case (field, value) => field -> value.toBoolean }
  }

  // ----- GNUPLOT CONFIGURATION ----- //

  // Upper bound of processed task during the run
  lazy val gnuplotMaxTaskDoneNumber: Int =
    (this fields "gnuplot-max-taskdone-number").toInt

  // Title of the plot
  lazy val gnuplotTitle: String = this fields "gnuplot-title"

  // Output filename
  lazy val gnuplotOutputFilename: String = this fields "gnuplot-output-filename"

  // Output format
  lazy val gnuplotOutputFormat: String = this fields "gnuplot-output-format"

  // ----- MONITOR CONFIGURATION ----- //

  // Monitor
  lazy val taskMonitor: Boolean = (this fields "task-monitor").toBoolean

  // Scale of a task on the monitor
  lazy val monitorTaskScale: Int = (this fields "monitor-task-scale").toInt

  // Step scale of a task on the monitor
  lazy val monitorTaskScaleStep: Int =
    (this fields "monitor-task-scale-step").toInt

}
