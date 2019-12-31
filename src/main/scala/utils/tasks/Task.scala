package utils.tasks

import akka.actor.ActorRef
import utils.config.ConfigurationBuilder
import utils.strategies.taskCost.TaskCostStrategy

import scala.collection.mutable

/** Task to perform.
  *
  * @param key    key represented by this task
  * @param chunks chunks of the task
  */
@SerialVersionUID(1L)
abstract case class Task(
  key: String,
  chunks: List[Chunk]
) extends Serializable with WithChunks {

  // Task cost strategy
  private lazy val taskCostStrategy: TaskCostStrategy =
    ConfigurationBuilder.config.taskCostStrategy

  // Stock the cost of the task for a given agent
  private val costForAgent: mutable.Map[ActorRef, Long] = mutable.Map()

  // Stock the ownership rate of the task for a given agent
  private val ownershipRateForAgent: Map[ActorRef, Double] = {
    val totalNbOfChunks = this.chunks.size

    this.chunks.groupBy(_.owner).map {
      case (owner, ownerChunks) =>
        val toRound = ownerChunks.size.toDouble / totalNbOfChunks

        owner -> utils.roundUpDouble(toRound, 2)
    } withDefaultValue 0.toDouble
  }

  /** Is the task an FR task ? */
  val isFR: Boolean = true

  /** Cost of the task.
    *
    * @param agent agent for which the task cost is computed
    * @return the task cost for the given agent
    */
  def cost(agent: ActorRef): Long = {
    if (!(this.costForAgent contains agent)) {
      this.costForAgent.update(
        agent,
        this.taskCostStrategy.getTaskCost(this, agent)
      )
    }

    this.costForAgent(agent)
  }

  /** Ownership rate of the task for a given agent.
    *
    * @param agent agent for which the ownership rate is computed
    * @return the task ownership for the given agent
    */
  def ownershipRate(agent: ActorRef): Double = this.ownershipRateForAgent(agent)

  /** Maximum ownership rate of the task. */
  val maximumOwnershipRate: Double = this.ownershipRateForAgent.values.max

  /** Number of values of the task. */
  lazy val nbValues: Long = this.chunks.foldLeft(0.toLong) {
    case (cost, chunk) => cost + chunk.nbValues.toLong
  }

  def canEqual(a: Any): Boolean = a.isInstanceOf[Task]

  override def equals(that: Any): Boolean = that match {
    case that: Task =>
      (that canEqual this) &&
      (that.key == this.key) &&
      (that.chunks == this.chunks)

    case _          => false
  }

  override def hashCode: Int =
    41 * (41 * (41 + this.chunks.hashCode) + this.key.hashCode)

}

/** Represent an IR task.
  *
  * @param key            key represented by this task
  * @param chunks         chunks of the task
  * @param frCost         cost of the original FR task
  * @param finalPerformer final performer of the IR task
  */
class IrTask(
  override val key: String,
  override val chunks: List[Chunk],
  val frCost: Long,
  val finalPerformer: ActorRef
) extends Task(key, chunks) with Divisible {

  override val isFR: Boolean = false

}
