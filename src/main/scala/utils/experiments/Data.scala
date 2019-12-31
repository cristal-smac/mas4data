package utils.experiments

import utils.files.FileWriter

import java.io.File

/** Represent the data gathered by an agent during a run. */
abstract class Data {

  // Fields of the data
  def fields: Map[String, String]

  /** Write these data in a file.
    *
    * @param filePath path of the file to create
    */
  def write(filePath: String): Unit =
    FileWriter.writeWith(new File(filePath), Iterator(this.toString))

  override def toString: String = {
    val keys = this.fields.keys.toList.sorted
    val values = keys map { x => this.fields(x) }

    (keys mkString ";") + "\n" + (values mkString ";") + "\n"
  }

}
