package utils.experiments

import java.io.File
import scala.io.Source
import sys.process._

import utils.config.ConfigurationHandler
import utils.files.FileWriter

/** Build and launch a set of experiments.
  *
  * @param nbIteration            number of iteration for each configuration
  * @param targetFolderPath       path of the target folder (i.e. the folder
  *                               which will contain the run results)
  * @param configLocationFilePath path of the file which indicates the
  *                               configuration file location
  * @param metaConfigFilePath     path of the meta configuration file
  */
class ExperimentsBuilder(
  nbIteration: Int,
  targetFolderPath: String,
  configLocationFilePath: String,
  metaConfigFilePath: String
) {

  // Fields from the meta configuration file
  private val suffixesAndFields: Map[String, Map[String, String]] =
    new ConfigFileBuilder(this.metaConfigFilePath).getSuffixesAndFields.toMap

  // Prefix for the experiment folders
  private val experimentsFolderPrefix: String = "results_"

  // Configuration file location
  private val configLocationFile: File = new File(this.configLocationFilePath)

  // Create an experiment folder
  private def createExperimentFolder(folder: File): Unit = folder.mkdir

  // Run an experiment with a given command
  private def runExperiments(cmd: String): Unit = {
    // Save the content of the configuration location file
    val source = Source fromFile this.configLocationFile
    val initialConfigLocationFileContent = source.getLines.toList

    source.close

    // Create each experiment configuration
    val configs =
      this.suffixesAndFields.foldLeft(List[(String, String, Map[String, String])]()) {
        case (acc, (suffix, configFields)) =>
          val correctedSuffix = suffix.replace("/", "-")
          val ithConfigs = for {
            i <- 1 to this.nbIteration
            folderPath = utils.files.correctPath(
              this.targetFolderPath + "/" + this.experimentsFolderPrefix +
                correctedSuffix + i + "/"
            )
            configFilePath = utils.files.correctPath(folderPath + "/config.txt")
            correctFields = configFields.updated("result-path", folderPath)
          } yield (folderPath, configFilePath, correctFields)

          acc ++ ithConfigs
      }

    // Execute the command for each experiment configuration
    for (
      (folderPath, configFilePath, correctFields) <- scala.util.Random.shuffle(configs)
    ) {
      // Create the experiment result folder
      this createExperimentFolder new File(folderPath)
      // Create the configuration file
      ConfigurationHandler.buildConfigFrom(configFilePath, correctFields)
      // Modify the configuration file location
      FileWriter.writeWith(this.configLocationFile, Iterator(configFilePath))
      // Launch the command
      cmd.!
      // Move the log file in the current experiment folder
      ("mv logAdaptive.txt " + folderPath.toString).!
    }

    // Rewrite the initial configuration file location
    FileWriter.writeWith(
      this.configLocationFile,
      initialConfigLocationFileContent.toIterator
    )
  }

  /** Run the classic MapReduce. */
  def runClassic(): Unit = this runExperiments "./scripts/runClassic.sh"

  /** Run the adaptive MapReduce. */
  def runAdaptive(): Unit = this runExperiments "./scripts/runAdaptive.sh"

}
