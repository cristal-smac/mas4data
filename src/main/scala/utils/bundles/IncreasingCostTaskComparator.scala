package utils.bundles

import utils.tasks.Task

/** Ordering which orders tasks using a given cost function.
  *
  * @param costFunction cost function to use to compute the task costs
  */
class IncreasingCostTaskComparator(costFunction: Task => Long)
  extends java.util.Comparator[Task] {

  /** Compare two tasks using the instance cost function.
    *
    * @param x first task
    * @param y second task
    * @return -1 if x is smaller than y, 1 x is greater than y, 0 if x equals y
    */
  def compare(x: Task, y: Task): Int = {
    val xCost = this.costFunction(x)
    val yCost = this.costFunction(y)

    if (xCost < yCost)
      -1
    else if (xCost > yCost)
      1
    else if (x == y)
      0
    else
      x.key.compareTo(y.key)
  }

}
