/** Contain the different implementations of the map reduce system. */
package object mapreduce {

  /** Partition keys among the reducers.
    *
    * @param key key to associate to a reducer
    * @param reducersAmount amount of reducers in the map reduce system
    * @return number of the reducer which will treat the key
    */
  def partition[K](key: K, reducersAmount: Int): Int =
    ((key.hashCode).abs) % reducersAmount

}
