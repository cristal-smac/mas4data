package utils.bundles

import akka.actor.ActorRef
import utils.config.ConfigurationBuilder
import utils.tasks.Task

import scala.collection.mutable
import scala.collection.JavaConverters._

/** Task bundle which give tasks to delegate or perform regarding their
  * ownership rate and their cost.
  *
  * @param initialTasks initial tasks to put in the bundle
  * @param owner        owner of the bundle
  * @param rfhMap       map which associates each agent with its RFH
  */
class OwnershipRateTaskBundle(
  initialTasks: List[Task],
  owner: ActorRef,
  rfhMap: Map[ActorRef, ActorRef]
) extends TaskBundle(initialTasks, owner, rfhMap) {

  private lazy val threshold: Double = ConfigurationBuilder.config.threshold

  private val increasingCostTaskComparator = new IncreasingCostTaskComparator(
    this.getTaskCostForOwner
  )

  /** Comparator class which order double values in decreasing order. */
  protected class DecreasingDoubleComparator
    extends java.util.Comparator[Double] {

    override def compare(o1: Double, o2: Double): Int = -o1.compareTo(o2)

  }

  /** Comparator class which order task in decreasing cost order. */
  protected class DecreasingCostTaskComparator
    extends java.util.Comparator[Task] {

    override def compare(o1: Task, o2: Task): Int =
      -OwnershipRateTaskBundle.this.increasingCostTaskComparator.compare(o1, o2)

  }

  /** Bundle which contains all the tasks which have a maximum ownership rate
    * for the owner.
    */
  protected val maxOwnershipBundle: java.util.TreeSet[Task] =
    new java.util.TreeSet(new DecreasingCostTaskComparator())

  /** Bundle which contains all the tasks which have a intermediate ownership
    * rate for the owner.
    */
  protected val intermedOwnershipBundle: java.util.TreeMap[Double, java.util.TreeSet[Task]] =
    new java.util.TreeMap[Double, java.util.TreeSet[Task]](
      new DecreasingDoubleComparator
    )

  /** Bundle which contains all the tasks which are non local
    * (i.e. ownership rate = 0) for the owner.
    */
  protected val nonLocalBundle: java.util.TreeSet[Task] =
    new java.util.TreeSet(this.increasingCostTaskComparator)

  /** Contains the number of tasks in the intermediate bundle. */
  protected var intermedBundleSize: Int = 0

  // Initiate the task bundle
  this.initialTasks foreach this.addTask

  /** Get the appropriate intermediate bundle for a given task.
    *
    * @param task task to search the appropriate intermediate bundle for
    */
  protected def getIntermedBundleForTask(
    task: Task
  ): java.util.TreeSet[Task] = {
    val ownershipRateForOwner = this.getTaskOwnershipRateForOwner(task)

    if (
      !this.intermedOwnershipBundle.containsKey(ownershipRateForOwner)
    ) {
      this.intermedOwnershipBundle.put(
        ownershipRateForOwner,
        new java.util.TreeSet(new DecreasingCostTaskComparator)
      )
    }

    this.intermedOwnershipBundle.asScala(ownershipRateForOwner)
  }

  /** Return true iff the task bundle is empty.
    *
    * @return true iff the task bundle is empty
    */
  override def isEmpty: Boolean = {
    this.maxOwnershipBundle.isEmpty  &&
      (this.intermedBundleSize == 0) &&
      this.nonLocalBundle.isEmpty
  }

  /** Return the size of the task bundle (i.e. the number of tasks in the
    * bundle).
    *
    * @return the size of the task bundle
    */
  override def size: Int = {
     this.maxOwnershipBundle.size +
       this.intermedBundleSize    +
       this.nonLocalBundle.size
  }

  /** Select the bundle to look into regarding the given task and the ownership
    * rate of the task bundle owner.
    *
    * @param task task used for computing the ownership rate
    * @return the appropriate task bundle to look into and true iff the task
    *         bundle is the intermediate one
    */
  protected def selectBundleAndInformIfItIsIntermedBundle(
    task: Task
  ): (java.util.TreeSet[Task], Boolean) = {
    val ownershipRateForOwner = this.getTaskOwnershipRateForOwner(task)

    if (ownershipRateForOwner == 0) {
      (this.nonLocalBundle, false)
    } else if (ownershipRateForOwner == task.maximumOwnershipRate) {
      (this.maxOwnershipBundle, false)
    } else {
      (this.getIntermedBundleForTask(task), true)
    }
  }

  /** Select the bundle to look into regarding the given task and the ownership
    * rate of the task bundle owner.
    *
    * @param task task used for computing the ownership rate
    * @return the appropriate task bundle to look into
    */
  protected def selectBundle(task: Task): java.util.TreeSet[Task] =
    this.selectBundleAndInformIfItIsIntermedBundle(task)._1

  /** Return true iff the task bundle contains the given task.
    *
    * @param task task to know if it is in the bundle or not
    * @return true iff the task bundle contains the given task
    */
  override def containsTask(task: Task): Boolean =
    this.selectBundle(task).contains(task)

  /** Inner process to add a task to the task bundle.
    *
    * @param task task to add to the bundle
    */
  override protected def addTaskToBundle(task: Task): Unit = {
    val (bundle, isIntermediate) =
      this.selectBundleAndInformIfItIsIntermedBundle(task)

    if (isIntermediate) this.intermedBundleSize += 1
    bundle.add(task)
  }

  /** Inner process to remove a task from the task bundle.
    *
    * @param task task to remove from the bundle
    */
  override protected def removeTaskFromBundle(task: Task): Unit = {
    val (bundle, isIntermediate) =
      this.selectBundleAndInformIfItIsIntermedBundle(task)

    if (isIntermediate) this.intermedBundleSize -= 1
    bundle.remove(task)
  }

  /** Return the next task to delegate.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to delegate
    */
  override def nextTaskToDelegate(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task] = {
    // Find the first task to delegate in a given bundle
    def findFirstTaskToDelegateInBundle(
      bundle: java.util.NavigableSet[Task]
    ): Option[Task] = bundle.asScala find {
      task => workloadMap exists {
        case (agent, workload) => this.canTakeTaskInCharge(
          currentOwnerWorkload,
          workload,
          this.getTaskCostFor(task, agent),
          this.threshold
        )
      }
    }

    // Try to find a task which is non local and for which at least one agent is
    // able to accept
    val taskToDelegateInNonLocalBundle =
      findFirstTaskToDelegateInBundle(this.nonLocalBundle.descendingSet())

    if (taskToDelegateInNonLocalBundle.isDefined) {
      taskToDelegateInNonLocalBundle
    }
    // Try to find a task which is partially local and for which at least one
    // agent is able to accept
    else {
      // Recursive function to find the first task to delegate in the
      // intermediate task bundle
      def recursiveFindTask(
        bundles: List[java.util.TreeSet[Task]],
        maybeTask: Option[Task]
      ): Option[Task] = if (bundles.nonEmpty && maybeTask.isEmpty) {
        val bundle = bundles.head
        val maybeTaskInBundle = findFirstTaskToDelegateInBundle(bundle)

        recursiveFindTask(bundles.tail, maybeTaskInBundle)
      } else {
        maybeTask
      }

      val intermedBundlesInDelegOrder =
        this.intermedOwnershipBundle.descendingKeySet().asScala.map {
          x => this.intermedOwnershipBundle.get(x)
        }
      val taskToDelegateInIntermedBundle =
        recursiveFindTask(intermedBundlesInDelegOrder.toList, None)

      if (taskToDelegateInIntermedBundle.isDefined) {
        taskToDelegateInIntermedBundle
      }
      // Try to find a task which has the maximum ownership rate for the owner
      // and for which at least one agent is able to accept
      else {
        findFirstTaskToDelegateInBundle(this.maxOwnershipBundle.descendingSet())
      }
    }
  }

  /** Return the next task to perform.
    *
    * @param currentOwnerWorkload current workload of the task bundle owner
    * @param workloadMap          map which associates agents with their
    *                             workload
    * @return the next task to perform
    */
  override def nextTaskToPerform(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task] = {
    if (this.maxOwnershipBundle.asScala.nonEmpty) {
      this.maxOwnershipBundle.asScala.headOption
    } else if (this.intermedBundleSize != 0) {
      this.intermedOwnershipBundle.asScala collectFirst {
        case (_, bundle) if bundle.asScala.nonEmpty => bundle.asScala.head
      }
    } else {
      this.nonLocalBundle.asScala.headOption
    }
  }

  /** Show the biggest task of the bundle.
    *
    * @return the biggest task of the bundle
    */
  override def showBiggestTask: Task = {
    val biggestInMaxOwnershipBundle = this.maxOwnershipBundle.asScala.headOption
    val biggestInNonLocalBundle = this.nonLocalBundle.asScala.lastOption
    val biggestInIntermedBundle = for (
      ownershipRate <- this.intermedOwnershipBundle.keySet().asScala
    ) yield this.intermedOwnershipBundle.get(ownershipRate).asScala.headOption
    val allPossibleBiggestTasks = {
      biggestInIntermedBundle       +
        biggestInMaxOwnershipBundle +
        biggestInNonLocalBundle
    }

    allPossibleBiggestTasks collect {
      case task if task.isDefined => task.get
    } maxBy {
      this.getTaskCostForOwner
    }
  }

}
