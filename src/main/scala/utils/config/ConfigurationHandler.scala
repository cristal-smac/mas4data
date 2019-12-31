package utils.config

import java.io.File
import scala.io.Source

import utils.files.FileWriter

/** Handle the configuration of a run. */
object ConfigurationHandler {

  private val separator = ":"

  private val comment = "//"

  /** Provide the configuration for a run.
    *
    * @param configFilePath path of the configuration file
    * @return some configuration if the configuration file exists, nothing
    *         otherwise
    */
  def getConfigFrom(configFilePath: String): Map[String, String] = {
    val source = Source fromFile new File(configFilePath)
    val allLines = source.getLines
    val usefulLines = allLines filter { line =>
      (line contains this.separator) && !(line startsWith this.comment)
    }
    val result = (usefulLines map { line => {
      val splitLine = line split this.separator

      (splitLine(0).trim, splitLine(1).trim)
    } } ).toMap

    source.close
    result
  }

  /** Create a configuration file from a fields and values map.
    *
    * @param configFilePath path of the created configuration file
    * @param fields         fields of the created configuration file
    */
  def buildConfigFrom(
    configFilePath: String,
    fields: Map[String, String]
  ): Unit = {
    val text = (fields map {
      case (field, value) => field + " " + this.separator + " " + value + "\n"
    }).toIterator
    val file = new File(configFilePath)

    FileWriter.writeWith(file, text)
  }

}
