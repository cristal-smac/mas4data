package utils.strategies.split

/** Represent a task split permission. */
abstract class TaskSplitPermission {

  /** Determine if the reducer is the most loaded of its acquaintances.
    *
    * @param ownContribution contribution of the task splitter agent
    */
  def canInitiateSplit(ownContribution: Long): Boolean

}

/** Task split allowed. */
object TaskSplitAllowed extends TaskSplitPermission {

  def canInitiateSplit(ownContribution: Long): Boolean = true

}

/** Task split denied. */
object TaskSplitDenied extends TaskSplitPermission {

  def canInitiateSplit(ownContribution: Long): Boolean = false

}
