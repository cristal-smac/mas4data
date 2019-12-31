package utils.jobs

/** The result of a map job is a map which contains a key and a list of
  * associated values.
  */
case class MapResult[MK, MV](val key: MK, val values: List[MV]) {

  def toIterator: Iterator[String] =
    (this.key.toString :: (this.values map { _.toString })).toIterator

}

/** The result of a reduce job is a map which contains keys and
  * associated values.
  */
class ReduceResult[K, V](val key: K, val value: V)
