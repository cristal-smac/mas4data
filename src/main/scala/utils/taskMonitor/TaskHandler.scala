package utils.taskMonitor

import mapreduce.adaptive.Manager
import utils.tasks.Task

/**
  * This stackable trait defines the set of methods that override Manager's
  * methods to add * the sending of message when some task event occurs : task
  * is added or removed, being performed or done. Methods are decorated with a
  * message sent to the adaptive reducer (the parent) in order to inform it
  * about the event.
  *
  * @author JC
  */
trait TaskHandler extends Manager {

//  abstract override def removeTask(task: Task): Unit = {
//    val list = super.removeTask(task)
//    context.parent ! TaskEventHandler.TaskRemoved(list, task)
//    list
//  }

//  abstract override def addTask(task: Task): Unit = {
//    val list = super.addTask(task)
//    context.parent ! TaskEventHandler.TaskAdded(list, task)
//    list
//  }

  abstract override def giveToWorker(dataToProcess: Task, state: String): Unit = {
    context.parent ! TaskEventHandler.TaskPerformed(dataToProcess)
    super.giveToWorker(dataToProcess, state)
  }

  abstract override def workerHasDone(): Unit = {
    context.parent ! TaskEventHandler.TaskDone
    super.workerHasDone()
  }

}
