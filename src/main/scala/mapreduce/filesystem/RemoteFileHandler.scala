package mapreduce.filesystem

import java.io._

import akka.actor.{Actor, ActorRef, Props}
import mapreduce.adaptive.Monitor
import utils.config.{Configuration, ConfigurationBuilder}
import utils.tasks.{Chunk, FileSource, MemorySource}

import scala.collection.mutable

/** Companion object of the RemoteFileHandler agent.
  *
  * Contains all the messages that a RFH can receive or send.
  */
object RemoteFileHandler {

  /** Message GetDataFor
    *
    * Request from an agent of the system to get some chunks locally.
    *
    * @param chunks chunks to get
    */
  case class GetDataFor(chunks: List[Chunk])

  /** Message GetDataForTask
    *
    * Request from a RFH to its ChunkProvider to get chunks of the task it has
    * been built for.
    */
  case object GetDataForTask

  /** Message AvailableDataFor
    *
    * The RFH uses this message to inform an agent that chunks are available
    * locally.
    *
    * @param chunks locally available chunks
    */
  case class AvailableDataFor(chunks: List[Chunk])

  /** Message WantDataForChunk
    *
    * Request from a RFH to another to obtain a chunk.
    *
    * @param chunk chunk to get
    */
  case class WantDataForChunk(chunk: Chunk)

  /** Message WantDataForFile
    *
    * Request from a RFH to another to obtain a file.
    *
    * @param filePath path of the requested file
    */
  case class WantDataForFile(filePath: String)

  /** Message CheckChunks
    *
    * Internal message to check if chunks are locally available or not.
    */
  case class CheckChunks(chunks: (ActorRef, List[Chunk]))

  /** Message ProvideData
    *
    * Message to provide data previously requested by a RFH.
    *
    * @param content   requested content
    * @param remaining does it remains data to provide?
    */
  case class ProvideData(content: Array[Byte], remaining: Boolean)

  /** Message DataOk
    *
    * Acknowledgement of the ProvideData message.
    */
  case object DataOk

  /** Message GotAllData
    *
    * Puts an end to a exchange between RFH.
    */
  case object GotAllData

  /** Message GetResFiles
    *
    * Request from the monitor to its RFH to get the results files.
    *
    * @param rfhs RFH from which get the result files
    */
  case class GetResFiles(rfhs: List[ActorRef])

  /** Message GetResFilesFor
    *
    * Message from a RFH to one of its sub-agent to initiate a request for
    * results files.
    *
    * @param rfh remote RFH to ask the result files for
    */
  case class GetResFilesFor(rfh: ActorRef)

  /** Message ListResFiles
    *
    * Request from a RFH to another to get the list a the results files the
    * second RFH has.
    */
  case object ListResFiles

  /** Message ResFiles
    *
    * Answer to the ListResFiles message.
    *
    * @param resFiles result file names
    */
  case class ResFiles(resFiles: List[String])

  /** Message AskFile
    *
    * Internal message to initiate a results files sending.
    *
    * @param filePath requested file path
    */
  case class AskFile(filePath: String)

  /** Message FilesAvailable
    *
    * Message sent to the monitor to inform that the result files are available.
    */
  case object FilesAvailable

  /** Message ConfigureEnvironment
    *
    * Request from the monitor to copy the configuration object on a remote node
    * through its RFH.
    *
    * @param config     configuration object
    * @param configPath path of the configuration file which has to be written
    * @param resultPath result path for the remote node
    */
  case class ConfigureEnvironment(
    config: Configuration,
    configPath: String,
    resultPath: String
  )

  /** Message RemoveResults
    *
    * Request from the monitor to a remote RFH to remove the results folder.
    */
  case object RemoveResults

}

/** RemoteFileHandler agent.
  *
  * Handle the chunks exchange between nodes of the system.
  */
class RemoteFileHandler extends Actor with utils.debugs.Debug {

  import RemoteFileHandler._

  private var config: Configuration = _

  // Return true iff the file name is a result file name
  private def isResFile(fileName: String): Boolean =
    (fileName contains "worker@") ||
    (fileName endsWith ".dat")    ||
    (fileName endsWith ".csv")

  // Counters used to name sub-agents
  private val counters: mutable.Map[String, Int] =
    mutable.Map() withDefaultValue 0

  // Remove the result folder of the RFH node
  private def removeResultsFolder(): Unit = {
    val resultFolder = new File(this.config.resultPath)

    resultFolder.listFiles foreach { _.delete }
    resultFolder.delete
  }

  // Generate a name for a sub-agent
  private def generateName(agentType: String): String = {
    val name = self.path.name + "@" + agentType + this.counters(agentType)

    this.counters.update(agentType, this.counters(agentType) + 1)
    name
  }

  def receive: Receive = {

    // A local agent need chunks
    case GetDataFor(chunks) =>
      debugReceive("GetDataFor", sender, "receive")

      // Create a chunk provider to deal with the request
      val provider = context.actorOf(
        Props(classOf[ChunkProvider], chunks, sender),
        name = this.generateName("chunkProvider")
      )

      debugSend("GetDataForTask", "receive")
      provider ! GetDataForTask

    // A remote RFH needs chunks or files
    case msg@(WantDataForChunk(_) | WantDataForFile(_))  =>
      debugReceive("WantDataForChunk | WantDataForFile", sender, "receive")

      // Create a file handler to deal with the request
      val handler = context.actorOf(
        Props(classOf[RequestHandler]),
        name = this.generateName("requestHandler")
      )

      handler forward msg

    // The monitor asks for the result files
    case GetResFiles(rfhs) =>
      debugReceive("GetResFiles", sender, "receive")
      // Create one file provider by remote RFH to deal with the request
      rfhs foreach {
        rfh =>
          val provider = context.actorOf(
            Props(classOf[ResultFilesProvider], sender),
            name = this.generateName("resultFilesProvider")
          )

          provider ! GetResFilesFor(rfh)
      }

    // A remote RFH asks the list of the local result files
    case ListResFiles =>
      debugReceive("ListResFiles", sender, "receive")

      val resFolder = new File(this.config.resultPath)
      val resFolderFilesNames = resFolder.listFiles map { _.getPath }
      val resFiles = resFolderFilesNames filter this.isResFile

      sender ! ResFiles(resFiles.toList)

    // The monitor asks for a configuration change
    case ConfigureEnvironment(remoteConfig, configPath, resultPath) =>
      debugReceive("ConfigureEnvironment", sender, "receive")

      // Create necessary configuration and results folder
      val resultFolder = new File(resultPath)
      val stringConfigFolder = (configPath split "/").init mkString "/"
      val configFolder = new File(stringConfigFolder)

      if (!resultFolder.exists) resultFolder.mkdirs
      if (!configFolder.exists) configFolder.mkdirs

      val os = new ObjectOutputStream(new FileOutputStream(configPath))

      // Write configuration object
      os.writeObject(remoteConfig)
      os.close()
      this.config = ConfigurationBuilder.config
      this.setDebug(this.config.debugs("debug-rfh"))
      debugSend("ConfigOk", "active")
      sender ! Monitor.ConfigOk

    // The monitor asks for the result files to be removed
    case RemoveResults =>
      debugReceive("RemoveResults", sender, "receive")
      this.removeResultsFolder()
      sender ! Monitor.ResultsRemoved
      context stop self

    case msg@_ => this.debugUnexpected(self, sender, "receive", msg)

  }

}

/** ChunkProvider agent.
  *
  * Provide chunks to local agents. Make request to remote RFH if the chunk data
  * are not known.
  *
  * @param chunks   chunks to provide
  * @param enquirer local agent which needs the chunks
  */
class ChunkProvider(
  chunks: List[Chunk],
  enquirer: ActorRef
) extends Actor with utils.debugs.Debug {

  import RemoteFileHandler._

  this.setDebug(ConfigurationBuilder.config.debugs("debug-rfh"))

  // Inform the enquirer by providing the a new task object where all chunks are
  // local
  private def informEnquirer(chunks: List[Chunk], state: String): Unit = {
    debugSend("AvailableDataFor", state)
    enquirer ! AvailableDataFor(chunks)
  }

  private def checkEndOfRequest(
    chunks: List[(ActorRef, List[Chunk])],
    availableChunks: List[Chunk]
  ): Unit = {
    if (chunks.isEmpty) {
      this.informEnquirer(availableChunks, "providing")
      context stop self
    } else {
      context become (
        this.providing(chunks.tail, availableChunks)
          orElse this.handleUnexpected("providing")
        )
      self ! CheckChunks(chunks.head)
    }
  }

  def receive: Receive = {

    // A local agents asks for the given task chunks
    case GetDataForTask =>
      debugReceive("GetDataForTask", sender, "receive")

      val chunksByOwner = this.chunks.groupBy(_.owner).toList

      context become (
        this.providing(chunksByOwner.tail, List[Chunk]())
        orElse this.handleUnexpected("providing")
      )
      self ! CheckChunks(chunksByOwner.head)

  }

  /** State in which the chunk provider provides chunks to a local agent (it
    * requests chunks to remote RFH if they are not local).
    *
    * @param chunks          remaining chunks to provide
    * @param availableChunks available chunks
    */
  def providing(
    chunks: List[(ActorRef, List[Chunk])],
    availableChunks: List[Chunk]
  ): Receive = {

    // The chunks are local
    case CheckChunks((owner, ownerChunks)) if owner == context.parent =>
      debugReceive(
        "CheckChunks " + chunks + " : hasChunk = true",
        sender,
        "providing"
      )
      this.checkEndOfRequest(chunks, availableChunks ++ ownerChunks)


    // The chunks are not local, the chunk provider need to ask it to a remote
    // RFH
    case CheckChunks((owner, ownerChunks)) =>
      debugReceive(
        "CheckChunks " + chunks + " : hasChunk = false",
        sender,
        "providing"
      )

      val (localChunks, remoteChunks) = ownerChunks span {
        chunk => chunk.dataSource match {
          case MemorySource(_)      => true
          case FileSource(filePath) => new File(filePath).exists
        }
      }
      val newAvailableChunks = availableChunks ++ localChunks

      if (remoteChunks.nonEmpty) {
        val currentChunk = remoteChunks.head

        debugSend("WantDataForChunk " + currentChunk, "providing")
        owner ! WantDataForChunk(currentChunk)
        context become (
          this.waitingChunks(
            currentChunk,
            remoteChunks.tail,
            chunks,
            newAvailableChunks,
            Array[Byte]()
          )
            orElse this.handleUnexpected("waitingChunks")
          )
      } else {
        this.checkEndOfRequest(chunks, newAvailableChunks)
      }

  }

  /** State in which the file provider waits for chunks from a remote RFH.
    *
    * @param currentChunk    currentChunk which the ChunkProvider waits for
    * @param ownerChunks     chunks that are owned by the current remote RFH
    * @param chunks          chunks the ChunkProvider has to provide
    * @param availableChunks available chunks
    */
  def waitingChunks(
    currentChunk: Chunk,
    ownerChunks: List[Chunk],
    chunks: List[(ActorRef, List[Chunk])],
    availableChunks: List[Chunk],
    buffer: Array[Byte]
  ): Receive = {

    // The remote RFH provide the last piece of data about the current chunk
    case ProvideData(bytes, false) =>
      debugReceive("ProvideChunks remaining=false", sender, "waitingChunks")
      debugSend("DataOk", "waitingChunks")
      sender ! DataOk

      val dataSource = MemorySource(buffer ++ bytes)
      val chunk = new Chunk(dataSource, currentChunk.nbValues, context.parent)
      val newAvailableChunks = chunk :: availableChunks

      // If there is no more chunk from the current remote RFH...
      if (ownerChunks.isEmpty) {
        debugSend("GotAllData", "waitingChunks")
        sender ! GotAllData

        // ... and there is no more chunks to ask for
        if (chunks.isEmpty) {
          this.informEnquirer(newAvailableChunks, "waitingChunks")
          context stop self
        }
        // ... but it remains chunks to ask for
        else {
          debugSend("CheckChunks", "waitingChunks")
          self ! CheckChunks(chunks.head)
          context become (
            this.providing(chunks.tail, newAvailableChunks)
            orElse this.handleUnexpected("providing")
          )
        }
      }
      // If it remains chunks from the current remote RFH
      else {
        val newCurrentChunk = ownerChunks.head

        debugSend("WantDataFor", "waitingChunks")
        sender ! WantDataForChunk(newCurrentChunk)
        context become (
          this.waitingChunks(
            newCurrentChunk,
            ownerChunks.tail,
            chunks,
            newAvailableChunks,
            Array[Byte]()
          ) orElse this.handleUnexpected("waitingChunks")
        )
      }

    case ProvideData(bytes, true) =>
      debugReceive("ProvideChunks remaining=true", sender, "waitingChunks")
      debugSend("DataOk", "waitingChunks")
      sender ! DataOk
      context become (
        this.waitingChunks(
          currentChunk,
          ownerChunks,
          chunks,
          availableChunks,
          buffer ++ bytes
        ) orElse this.handleUnexpected("waitingChunks")
      )

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}

/** Agent which provide result files to a local agent.
  *
  * @param enquirer local agent which requests the result files
  */
class ResultFilesProvider(
  enquirer: ActorRef
) extends Actor with utils.debugs.Debug {

  import RemoteFileHandler._

  this.setDebug(ConfigurationBuilder.config.debugs("debug-rfh"))

  def receive: Receive = {

    case GetResFilesFor(rfh) =>
      debugReceive("GetResFiles for " + rfh, sender, "receive")
      debugSend("ListResFiles", "receive")
      rfh ! ListResFiles
      context become (
        this.waitResFilesList
        orElse this.handleUnexpected("waitResFilesList")
      )

  }

  /** State in which the ResultFilesProvider waits for result files list from
    * the remote RFH.
    */
  def waitResFilesList: Receive = {

    // There are result files
    case ResFiles(filePaths) if filePaths.nonEmpty =>
      debugReceive("ResFiles(" + filePaths + ")", sender, "waitResFilesList")
      context become (
        this.providingResFiles(filePaths.tail, sender)
        orElse this.handleUnexpected("providingResFiles")
      )
      debugSend("AskFile to self", "waitResFilesList")
      self ! AskFile(filePaths.head)

    // There is no result file
    case ResFiles(_) =>
      debugReceive("ResFiles with empty list", sender, "waitResFilesList")
      debugSend("FilesAvailable", "waitResFilesList")
      this.enquirer ! FilesAvailable
      context stop self

  }

  /** State in which the ResultFileProvider provides remote result files to a
    * local agent.
    *
    * @param filePaths remote result file paths
    * @param contact   remote agent which provide the result files
    */
  def providingResFiles(filePaths: List[String], contact: ActorRef): Receive = {

    case AskFile(filePath) =>
      debugReceive("AskFile", sender, "providingResFiles")
      debugSend("WantDataForFile", "providingResFiles")
      contact ! WantDataForFile(filePath)
      context become (
        this.waitResFile(filePath, filePaths, Array[Byte]())
        orElse this.handleUnexpected("waitResFile")
      )

  }

  /** State in which the ResultFileProvider waits for a particular remote result
    * file.
    *
    * @param currentPath path of the remote file
    * @param filePaths   remote result file paths
    */
  def waitResFile(
    currentPath: String,
    filePaths: List[String],
    buffer: Array[Byte]
  ): Receive = {

    case ProvideData(bytes, false) =>
      debugReceive(
        "ProvideData (remaining=false) for " + currentPath,
        sender,
        "waitResFile"
      )
      debugSend("DataOk", "waitResFile")
      sender ! DataOk

      val file = new File(currentPath)
      val fos = new FileOutputStream(file)

      // Write the result file locally
      fos.write(buffer ++ bytes)
      fos.close()

      // It remains remote result files
      if (filePaths.nonEmpty) {
        context become (
          this.providingResFiles(filePaths.tail, sender)
          orElse this.handleUnexpected("providingResFiles")
        )
        self ! AskFile(filePaths.head)
      }
      // All the result files are local
      else {
        debugSend("GotAllData", "waitResFile")
        sender ! GotAllData
        debugSend("FilesAvailable", "waitResFile")
        this.enquirer ! FilesAvailable
        context stop self
      }

    case ProvideData(bytes, true) =>
      debugReceive(
        "ProvideData (remaining=true) for " + currentPath,
        sender,
        "waitResFile"
      )
      debugSend("DataOk", "waitResFile")
      sender ! DataOk
      context become (
        this.waitResFile(currentPath, filePaths, buffer ++ bytes)
        orElse this.handleUnexpected("waitResFiles")
      )

  }

  /** Handle unexpected messages.
    *
    * @param state state in which the unexpected message is received
    */
  def handleUnexpected(state: String): Receive = {

    case msg@_ => debugUnexpected(self, sender, state, msg)

  }

}

/** FileHandler agent.
  *
  * Send chunks to another requesting RFH.
  */
class RequestHandler extends Actor with utils.debugs.Debug {

  import RemoteFileHandler._

  this.setDebug(ConfigurationBuilder.config.debugs("debug-rfh"))

  private val maxSize: Int = 25000000

  private def sendBytes(bytes: Array[Byte]): Unit = {
    if (bytes.lengthCompare(this.maxSize) < 0) {
      debugSend("ProvideData", "sending")
      sender ! ProvideData(bytes, remaining=false)
      context become (
        this.sending(Array[Byte]())
          orElse this.handleUnexpected("sending")
        )
    } else {
      val (toSend, toKeep) = bytes.splitAt(this.maxSize)

      debugSend("ProvideData", "sending")
      sender ! ProvideData(toSend, remaining=true)
      context become (
        this.sending(toKeep)
          orElse this.handleUnexpected("sending")
        )
    }
  }

  def receive: Receive =
    this.sending(Array[Byte]()) orElse this.handleUnexpected("sending")

  /** State in which the request handler sends data to remote agents. */
  def sending(bytesToSend: Array[Byte]): Receive = {

    case WantDataForChunk(chunk) =>
      debugReceive("WantDataForChunk", sender, "sending")
      this.sendBytes(chunk.dataSource.getBytes)

    case WantDataForFile(filePath) =>
      debugReceive("WantDataForFile", sender, "sending")
      this.sendBytes(
        java.nio.file.Files.readAllBytes(
          java.nio.file.Paths.get(filePath)
        )
      )

    case DataOk if bytesToSend.isEmpty =>
      debugReceive("DataOk", sender, "sending")

    case DataOk =>
      debugReceive("DataOk", sender, "sending")
      this.sendBytes(bytesToSend)

    case GotAllData =>
      debugReceive("GotAllData", sender, "sending")
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
