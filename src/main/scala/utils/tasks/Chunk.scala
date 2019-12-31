package utils.tasks

import akka.actor.ActorRef

import utils.config.ConfigurationBuilder

import scala.io.Source
import utils.jobs.ReduceJob

/** A chunk is a a file which represents a piece of input data for a reduce
  * task.
  *
  * @param dataSource data source of the chunk
  * @param nbValues   cost represented by the chunk
  * @param owner      RFH which locally has the chunk
  */
class Chunk(
  val dataSource: DataSource,
  val nbValues: Int,
  val owner: ActorRef
) extends Serializable {

  private lazy val reduceJob: ReduceJob =
    ConfigurationBuilder.config.reduceJob.getDeclaredConstructor().newInstance() match {
      case t: ReduceJob => t
    }

  /** Extract the data from the chunk. */
  def get: Iterator[String] = this.dataSource.get

  /** Volume in bytes of the chunk. */
  lazy val volume: Long = this.reduceJob.nbBytesOfOneValue * this.nbValues

  def canEqual(a: Any): Boolean = a.isInstanceOf[Chunk]

  override def equals(that: Any): Boolean = that match {
    case that: Chunk =>
      (that canEqual this) &&
      (that.dataSource == this.dataSource) &&
      (that.nbValues == this.nbValues) &&
      (that.owner == this.owner)

    case _           => false
  }

  override def hashCode: Int =
    41 * (41 * (41 + this.dataSource.hashCode) * this.nbValues.hashCode)

}
