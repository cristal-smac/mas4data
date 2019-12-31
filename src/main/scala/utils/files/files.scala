package utils

package object files {

  /** Correct a file path.
    *
    * @param path path to correct
    * @return corrected path
    */
  def correctPath(path: String): String = path.replace("//", "/")

}
