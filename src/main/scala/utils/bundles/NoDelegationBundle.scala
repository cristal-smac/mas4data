package utils.bundles

import akka.actor.ActorRef
import utils.tasks.Task

import scala.collection.mutable

trait NoDelegationBundle extends TaskBundle {

  override def nextTaskToDelegate(
    currentOwnerWorkload: Long,
    workloadMap: mutable.Map[ActorRef, Long]
  ): Option[Task] = None

}
