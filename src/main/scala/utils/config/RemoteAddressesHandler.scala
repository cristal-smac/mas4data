package utils.config

import java.io.File
import scala.io.Source

/** Handle the remote addresses settings for a run. */
object RemoteAddressesHandler {

  private val comment = "//"
  private val separator = " "

  /** Get mapper or reducer name and associated remote addresses.
    *
    * @param remoteFilePath path of the file which contains remote addresses
    * @return a map which associates agents with their remote addresses if the
    *         file exists, nothing otherwise
    */
  def getRemoteAddresses(remoteFilePath: String): List[(String, Int)] = {
    val source = Source fromFile new File(remoteFilePath)
    val result = (source.getLines filterNot {
      line => line startsWith comment
    } map {
      line =>
        val splitLine = line split this.separator

        (splitLine(0), splitLine(1).toInt)
    }).toList

    source.close
    result
  }

}
