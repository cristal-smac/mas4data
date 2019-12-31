package utils.jobs

import akka.actor.ActorRef

import scala.collection.mutable
import utils.config.{Configuration, ConfigurationBuilder}
import utils.tasks.{Chunk, FileSource, Task}

/** Job applied in a map reduce system. */
sealed abstract class Job extends Serializable {

  /** Key type. */
  type K

  /** Value type. */
  type V

}

abstract class MapJob extends Job {

  var seed: Option[Int] = _

  var mapperNb: Int = _

  /** Intermediate buffer to save values which will be saved together in a
    * chunk.
    */
  protected val buffer = mutable.Map.empty[K, (List[V], Int)]

  /** Aggregation of all the results produced by this map job. */
  protected val results = mutable.Map.empty[K, List[Chunk]]

  /** Keep indexes of the chunks produced for each key. */
  protected val indexes: mutable.Map[K, Int] =
    mutable.Map.empty[K, Int] withDefaultValue 0

  // Configuration object
  private val config: Configuration = ConfigurationBuilder.config

  lazy private val random = if (this.seed.isDefined) {
    new scala.util.Random(this.seed.get)
  } else {
    scala.util.Random
  }

  private def chunkMultiplicationProcess(key: K): Int =
    if (this.config.chunkMult) {
      val shiftList = this.config.getMultShiftForMapper(this.mapperNb)
      val keyHash = {
        val hash = key.hashCode.abs % this.config.nbMapper
        if (hash == 0) this.config.nbMapper else hash
      } 
  
      if (shiftList contains keyHash) {
        val (start, end) = this.config.nbMultChunks.get

        this.config.chunkBaseMult + start + this.random.nextInt((end - start) + 1)
      } else this.config.chunkBaseMult
    } else 1

  /** Produce a chunk.
    *
    * @param key      key associated to the chunk
    * @param nbValues number of values to put into the chunk
    * @param values   values to put into the chunk
    * @param mapper   mapper which executes the job
    * @param rfh      rfh of the mapper which executes the job
    */
  protected def produceChunk(
    key: K,
    nbValues: Int,
    values: List[V],
    mapper: ActorRef,
    rfh: ActorRef
  ): Chunk = {
    val mapResult = MapResult(key, values)
    val index = this.indexes(key)
    val path = this.config.resultPath + mapper.path.name + "_" + key + "_" +
      index
    val dataSource = FileSource.createFileSource(path, mapResult.toIterator)
    
    new Chunk(dataSource, nbValues, rfh)
  }

  /** Handle a new key-value couple during the production of the map phase
    * results.
    *
    * @param key    encountered key
    * @param value  encountered value
    * @param mapper mapper which executes the job
    * @param rfh    rfh of the mapper which executes the job
    */
  def handleKeyValueDuringMapResultProduction(
    key: K,
    value: V,
    mapper: ActorRef,
    rfh: ActorRef
  ): Unit = {
    if (this.buffer contains key) {
      val (values, cpt) = this.buffer(key)
      val newCpt = cpt + 1

      if (newCpt == this.config.chunkSize) {
        val nbOfWriting = this.chunkMultiplicationProcess(key)

        for (_ <- 1 to nbOfWriting) {
          val chunk = this.produceChunk(key, newCpt, value :: values, mapper, rfh)

          if (this.results contains key) {
            this.results.update(key, chunk :: this.results(key))
            this.indexes.update(key, this.indexes(key) + 1)
          } else {
            this.results += (key -> List(chunk))
            this.indexes += (key -> 1)
          }
        } 

        this.buffer -= key
      } else {
        this.buffer.update(key, (value :: values, newCpt))
      }
    } else {
      this.buffer += (key -> (List(value), 1))
    }
  }

  /** Map function.
    *
    * @param line line on which execute the map function
    * @return list a (key, value) pairs
    */
  protected def map(line: String): Iterator[(K, V)]

  /** Produce the map result on the given line and add it to the results
    * collection of the map job.
    *
    * @param line   line on which execute the map function
    * @param mapper mapper which execute the map job
    * @param rfh    remote file handler which will handle the created chunks
    */
  def produceMapResult(
    line: String,
    mapper: ActorRef,
    rfh: ActorRef
  ): Unit = {
    val result = this map line

    result foreach {
      case (key, value) =>
        this.handleKeyValueDuringMapResultProduction(key, value, mapper, rfh)
    }
  }

  /** Get the results of this map job.
    *
    * @param mapper mapper which applies the map job
    * @param rfh    remote file handler which will contains the chunks created
    *               by the mapper
    * @return results produced by this map job
    */
  // def getResults(mapper: ActorRef, rfh: ActorRef): Map[K, List[Chunk]] = {
  //   this.buffer foreach {
  //     case (key, (values, cpt)) =>
  //       val nbOfWriting = this.chunkMultiplicationProcess(key)

  //       for (_ <- 1 to nbOfWriting) {
  //         val chunk = this.produceChunk(key, cpt, values, mapper, rfh)

  //         if (this.results contains key) {
  //           this.results.update(key, chunk :: this.results(key))
  //           this.indexes.update(key, this.indexes(key) + 1)
  //         } else {
  //           this.results += (key -> List(chunk))
  //           this.indexes += (key -> 1)
  //         }
  //       }
  //   }

  //   this.results.toMap
  // }
  def getResults(mapper: ActorRef, rfh: ActorRef): Map[K, List[Chunk]] = {
    val chunkSize = this.config.chunkSize

    this.buffer foreach {
      case (key, (values, cpt)) =>
        val nbOfWriting = this.chunkMultiplicationProcess(key)
        val nbValues = cpt * nbOfWriting
        val allValues = Stream.continually(values).flatten.take(nbValues)
        val valuesGroupedByChunks = allValues.grouped(chunkSize)

        while (valuesGroupedByChunks.hasNext) {
          val thisChunkValues = valuesGroupedByChunks.next.toList
          val chunk = this.produceChunk(
            key,
            thisChunkValues.length,
            thisChunkValues,
            mapper,
            rfh
          )

          if (this.results contains key) {
            this.results.update(key, chunk :: this.results(key))
            this.indexes.update(key, this.indexes(key) + 1)
          } else {
            this.results += (key -> List(chunk))
            this.indexes += (key -> 1)
          } 
        }
    }

    this.results.toMap
  }

}

abstract class ReduceJob extends Job {

  // Map results relative

  /** Associated map job key type. */
  type MK

  /** Associated map job value type. */
  type MV

  /** Recover a map key from a string.
    *
    * @param key string map key
    * @return recovered map key
    */
  def recoverMapKey(key: String): MK

  /** Recover a map value from a string.
    *
    * @param value string map value
    * @return recovered map value
    */
  def recoverMapValue(value: String): MV

  /** Build a map result from a string iterator.
    *
    * @param it string iterator
    * @return map result corresponding to the string iterator
    */
  def recoverMapResults(it: Iterator[String]): MapResult[MK, MV] = {
    val key = this recoverMapKey it.next
    val values = it.foldLeft(List[MV]()) {
      case (list, stringValue) =>
        val value = this recoverMapValue stringValue

        value :: list
    }

    MapResult(key, values)
  }

  type IV

  // Counter used to get the progress of the reduce job
  private var counter: Int = 0

  /** Get the counter of the job. */
  def getCounter: Int = this.counter

  /** Aggregate two values.
    *
    * @param v1 first value to aggregate
    * @param v2 second value to aggregate
    * @return aggregation of v1 and v2
    */
  def aggregateValues(v1: IV, v2: IV): IV

  /** Neutral value for the function returned by foldOperation
    *
    * @return the neutral value for foldOperation
    * @see foldOperation
    */
  def neutral: IV

  /** Provides the elementary fold function used by this job to reduce data
    *
    * @return the elementary left fold operation applied on data
    */
  def foldOperation: (IV, MV) => IV

  /** Wrapped the foldOperation with a counter increment, in order to provide
    * work-in-progress information
    *
    * @return result of foldOperation
    * @see foldOperation
    */
  private def countableFoldOperation: (IV, MV) => IV =
    (a: IV, b: MV) => {
      this.counter += 1
      this.foldOperation(a, b)
    }

  /** Defines the operation to be applied on the fold result, by default:
    * identity function
    *
    * @param result result on which apply the postFold operation
    * @return the final job result
    */
  def postFold(result: IV): V

  /** Build the reduce key from the previous map key
    *
    * @param mapKey previous map key
    * @return built map key
    */
  def buildKey(mapKey: MK): K

  /** Apply the intermediate reduce function to the given task.
    *
    * @param task task on which apply the intermediate reduce function
    * @return intermediate reduce result
    */
  def intermediateReduce(task: Task): ReduceResult[K, IV] = {
    this.counter = 0

    val intermediateResult = task.chunks.foldLeft(this.neutral) {
      case (result, chunk) =>
        val mapResult = this recoverMapResults chunk.get

        mapResult.values.foldLeft(result)(this.countableFoldOperation)
    }
    val mapKey = this recoverMapKey task.key

    new ReduceResult(this.buildKey(mapKey), intermediateResult)
  }

  /** Apply the reduce function to the given task.
    *
    * @param task task on which apply the reduce function
    * @return the reduce result
    */
  def reduce(task: Task): ReduceResult[K, V] = {
    val intermediateResult = this intermediateReduce task
    val finalResult = this postFold intermediateResult.value

    new ReduceResult(intermediateResult.key, finalResult)
  }

  /** Transform results produced by the job in a string iterator.
    *
    * @param results results to transform
    * @return string iterator from the given result
    */
  def transformResults(results: List[ReduceResult[K, V]]): Iterator[String] =
    (results map {
      result => result.key.toString + " " + result.value.toString + "\n"
    }).toIterator

  /** Number of bytes represented by one value that the reduce job has to
    * process.
    *
    * @return number of bytes represented by one value
    */
  val nbBytesOfOneValue: Int

}

/** Stackable trait that slows down foldOperation adding a pause. */
trait Pause extends ReduceJob {

  private val config: Configuration = ConfigurationBuilder.config

  abstract override def foldOperation: (IV, MV) => IV =
    (a: IV, b: MV) => {
      val result = super.foldOperation(a, b)

      Thread.sleep(this.config.milliPause, this.config.nanoPause)
      result
    }

}

// PROVIDED TRAIT //

trait IntMapKey extends ReduceJob {

  type MK = Int

  def recoverMapKey(key: String): Int = key.toInt

}

trait StringMapKey extends ReduceJob {

  type MK = String

  def recoverMapKey(key: String): String = key

}

trait DoubleMapKey extends ReduceJob {

  type MK = Double

  def recoverMapKey(key: String): Double = key.toDouble

}

trait IntMapValue extends ReduceJob {

  type MV = Int

  def recoverMapValue(value: String): Int = value.toInt

}

trait DoubleMapValue extends ReduceJob {

  type MV = Double

  def recoverMapValue(value: String): Double = value.toDouble

}

trait DoubleIntTupleMapValue extends ReduceJob {

  type MV = (Double, Int)

  private val valueRegex = """\((-?\d+\.\d+),(\d+)\)""".r

  def recoverMapValue(value: String): (Double, Int) = value match {
    case valueRegex(x, y) => (x.toDouble, y.toInt)
    case _                => throw new Exception("unable to recover map value")
  }

}

trait IntIntTupleMapValue extends ReduceJob {

  type MV = (Int, Int)

  private val valueRegex = """\((\d+),(\d+)\)""".r

  def recoverMapValue(value: String): (Int, Int) = value match {
    case valueRegex(x, y) => (x.toInt, y.toInt)
    case _                => throw new Exception("unable to recover map value")
  }

}

trait IntKeyFromIntMapKey extends ReduceJob with IntMapKey {

  type K = Int

  def buildKey(mapKey:Int): Int = mapKey

}

trait StringKey extends ReduceJob {

  type K = String

  def buildKey(mapKey: MK): String = mapKey.toString

}

trait DoubleKeyFromDoubleMapKey extends ReduceJob with DoubleMapKey {

  type K = Double

  def buildKey(mapKey: Double): Double = mapKey

}

trait AddInt extends ReduceJob with IntMapValue {

  type IV = Int
  type V  = Int

  def foldOperation: (Int, Int) => Int =
    (value: Int, mapValue: Int) => { value + mapValue }

  def neutral = 0

  def aggregateValues(v1: Int, v2: Int): Int = v1 + v2

  def postFold(result: Int): Int = result

  override val nbBytesOfOneValue: Int = 4

}

trait AddDouble extends ReduceJob with DoubleMapValue {

  type IV = Double
  type V  = Double

  def foldOperation: (Double, Double) => Double =
    (value: Double, mapValue: Double) => { value + mapValue }

  def neutral = 0

  def aggregateValues(v1: Double, v2: Double): Double = v1 + v2

  def postFold(result: Double): Double = result

  override val nbBytesOfOneValue: Int = 8

}

trait MeanDouble extends ReduceJob with DoubleIntTupleMapValue {

  type IV = (Double, Int)
  type V  = Double

  def foldOperation: ((Double, Int), (Double, Int)) => (Double, Int) = {
    case ((value, count), (mapValue, mapCount)) =>
      (value + mapValue, count + mapCount)
  }

  def neutral: (Double, Int) = (0.0, 0)

  def aggregateValues(v1: (Double, Int), v2: (Double, Int)): (Double, Int) =
    (v1._1 + v2._1, v1._2 + v2._2)

  def postFold(result: (Double, Int)): Double = result._1 / result._2

  override val nbBytesOfOneValue: Int = 8

}

trait MeanInt extends ReduceJob with IntIntTupleMapValue {

  type IV = (Int, Int)
  type V = Int

  def foldOperation: ((Int, Int), (Int, Int)) => (Int, Int) = {
    case ((value, count), (mapValue, mapCount)) =>
      (value + mapValue, count + mapCount)
  }

  def neutral: (Int, Int) = (0, 0)

  def aggregateValues(v1: (Int, Int), v2: (Int, Int)): (Int, Int) =
    (v1._1 + v2._1, v1._2 + v2._2)

  def postFold(result: (Int, Int)): Int = result._1 / result._2

  override val nbBytesOfOneValue: Int = 4

}
