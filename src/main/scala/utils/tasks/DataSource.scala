package utils.tasks

import java.io.File

import utils.files.FileWriter

import scala.io.Source

/** Represent a data source. */
trait DataSource {

  /** Get the data source. */
  def get: Iterator[String]

  /** Get the data with an array of bytes. */
  def getBytes: Array[Byte]

}

/** Companion object of the FileSource class. */
object FileSource {

  /** Create a file source.
    *
    * @param path    path of the file
    * @param content content of the file
    * @return the file source
    */
  def createFileSource(path: String, content: Iterator[String]): FileSource = {
    val file = new File(path)

    FileWriter.writeWith(file, content.map(_ + "\n"))
    FileSource(path)
  }

}

/** Represent a file data source. */
case class FileSource(filePath: String) extends DataSource {

  /** Get the data source. */
  def get: Iterator[String] = Source.fromFile(this.filePath).getLines

  /** Get the data with an array of bytes. */
  def getBytes: Array[Byte] = java.nio.file.Files.readAllBytes(
    java.nio.file.Paths.get(filePath)
  )

}

object MemorySource {

  def createMemorySource(content: Iterator[String]): MemorySource =
    MemorySource(content.map(_ + "\n").flatMap(_.map(_.toByte)).toArray)

}

/** Represent a memory data source. */
case class MemorySource(bytes: Array[Byte]) extends DataSource {

  /** Get the data source. */
  def get: Iterator[String] = new String(this.bytes).split("\n").toIterator

  /** Get the data with an array of bytes. */
  def getBytes: Array[Byte] = this.bytes

}
