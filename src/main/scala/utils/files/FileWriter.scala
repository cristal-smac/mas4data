package utils.files

/** Write files. */
object FileWriter {

  import java.io.{File, BufferedWriter, FileWriter}

  /** Write a file with the given content.
    *
    * @param file    file to write the content in
    * @param content content to write in the file
    */
  def writeWith(file: File, content: Iterator[String]): Unit = {
    val bw = new BufferedWriter(new FileWriter(file))

    try {
      content foreach { line => bw write line }
    } finally {
      bw.close()
    }
  }

}
