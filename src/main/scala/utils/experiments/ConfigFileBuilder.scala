package utils.experiments

import utils.config.ConfigurationHandler

import scala.util.matching.Regex

/** Companion object of the ConfigFileBuilder. */
object ConfigFileBuilder {

  // A field which begins with this id is identified as meta field
  val metaFieldId = "*"

  // Seperator of the different values that could take a meta field
  val metaValueSeparator = ";"

  // Regex to identifies values of a meta field
  val metaValuesRegex: Regex = """\[(.*)\]""".r

  /** Isolate each values of a meta field.
    *
    * @param metaValues string representing all the meta values
    * @return all the values that could take the meta field
    */
  def metaValuesToValues(metaValues: String): List[String] = {
    val valuesWithComa = (
      this.metaValuesRegex findFirstMatchIn metaValues
    ).get.group(1)

    ((valuesWithComa split this.metaValueSeparator) map { _.trim }).toList
  }

  /** Return the cartesian product of several lists.
    *
    * @param lists lists on which make the cartesian product
    * @return cartesian product of the lists
    */
  def cartesianProduct[T](lists: List[List[T]]): List[List[T]] =
    lists match {
      case l::ls => for {
        x  <- l
        xs <- cartesianProduct(ls)
      } yield x :: xs

      case Nil   => List(Nil)
    }

}

/** Build a configuration object from a meta configuration file.
  *
  * @param metaConfigFilePath file path of the meta configuration file
  */
class ConfigFileBuilder(metaConfigFilePath: String) {

  // Meta fields and classical fields
  private val (metaFields, fields): (Map[String, String], Map[String, String]) = {
    val allFields = ConfigurationHandler getConfigFrom this.metaConfigFilePath

    allFields partition {
      case (field, _) => field startsWith ConfigFileBuilder.metaFieldId
    }
  }

  // Delete spaces from a line
  private def deleteSpace(line: String): String = line.replace(" ", "")

  // Create a suffix folder name from meta fields
  private def getSuffixFromMetaFields(metaFields: Map[String, String]): String = {
    val sortedFields = metaFields.toList sortWith {
      case ((field1, _), (field2, _)) => field1 < field2
    }

    (sortedFields map {
      case (field, value) => deleteSpace(field) + "-" + deleteSpace(value)
    }) mkString "_"
  }

  /** Return all the possible fields considering the meta configuration file and
    * their associate suffix folder name.
    *
    * @return all the possible fields considering the meta configuration file
    */
  def getSuffixesAndFields: List[(String, Map[String, String])] = {
    val metaFieldsAndValues = this.metaFields map {
      case (metaField, metaValues) =>
        metaField -> ConfigFileBuilder.metaValuesToValues(metaValues)
    }
    val metaFieldsAndValuesCouples = (metaFieldsAndValues map {
      case (metaField, values) =>
        val correctField =
          metaField.replace(ConfigFileBuilder.metaFieldId, "").trim

        values map { value => (correctField, value) }
    }).toList
    val configCombinations =
      ConfigFileBuilder cartesianProduct metaFieldsAndValuesCouples

    configCombinations.foldLeft(List[(String, Map[String, String])]()) {
      case (acc, config) =>
        val allFieldsWithConfig = this.fields ++ config
        val suffix = this getSuffixFromMetaFields config.toMap

        (suffix, allFieldsWithConfig) :: acc
    }
  }

}
