package utils.taskMonitor

import utils.tasks.Task

/**
 * @param reducerName the name of the reducer that emits this event
 * @param taskList the current tasks list of the reducer when the event is emitted
 * @param task the task that is at the origin of this event
 */
@SerialVersionUID(1L)
case class TaskEvent(
  reducerName: String,
  taskList: List[Task],
  task: Task
) extends Serializable
