package utils.tasks

import akka.actor.ActorRef

/** Represent an intermediate reduce results.
  *
  * @param key key of the result
  * @param values values of the result
  * @param irCost cost of the IR task from which the result come from
  * @param frCost cost of the FR task from which the result come from
  * @param finalPerformer final performer of the IR task
  */
@SerialVersionUID(1L)
case class IrResult(
  key: String,
  values: List[String],
  irCost: Long,
  frCost: Long,
  finalPerformer: ActorRef
) {

  /** Merge this IR result with an other IR result.
    * /!\ The two results has to be about the same key /!\
    *
    * @param ir IR result to merge with this IR result
    * @return a new IR result
    */
  def mergeWith(ir: IrResult): IrResult = {
    assert(ir.key == this.key)
    assert(ir.finalPerformer == this.finalPerformer)
    assert(ir.frCost == this.frCost)

    IrResult(
      this.key,
      this.values ++ ir.values,
      this.irCost + ir.irCost,
      this.frCost,
      this.finalPerformer
    )
  }

  /** Determine if this result can be transformed in a FR task.
    *
    * @return true iff this result can be transformed in a FR task
    */
  def canBeTransformedInFrTask: Boolean = this.irCost == this.frCost

  def canEqual(a: Any): Boolean = a.isInstanceOf[IrResult]

  override def equals(that: Any): Boolean = that match {
    case that: IrResult =>
      (that canEqual this) &&
      (that.key == this.key) &&
      (that.values == this.values) &&
      (that.irCost == this.irCost) &&
      (that.frCost == this.frCost) &&
      (that.finalPerformer == this.finalPerformer)

    case _              => false
  }

  override def hashCode: Int =
    41 * (41 * (41 + this.key.hashCode) + this.values.hashCode)

}
