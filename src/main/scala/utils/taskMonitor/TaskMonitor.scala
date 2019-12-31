package utils.taskMonitor

import akka.actor.Actor
import scala.collection.mutable.Map

import mapreduce.adaptive.Monitor
import utils.config.ConfigurationBuilder

import javax.swing._
import java.awt.{ Graphics, Graphics2D, Color, BorderLayout, GridLayout, Stroke, BasicStroke }
import java.awt.color._
import java.awt.event._
import java.util.Random

/**
 * This actor can listen to reducers to handle task event messages.
 * It displays a frame where the list of tasks of each reducer is displaid as
 * a stacked bar chart. Each task is shown as a colored rectangle. <br>
 *
 * For each reducer one can see : done tasks appeared framed in BarChartDisplay.DONE_TASK_COLOR, current worker task
 * is framed in BarChartDisplay.CURRENT_TASK_COLOR and still to do task in BarChartDisplay.TODO_TASK_COLOR
 *
 * Dynamic evolution of tasks for each reducer can be observed.
 */
class TaskMonitor extends Actor with TaskListener {

  private lazy val config = ConfigurationBuilder.config

  val frame: MyFrame = new MyFrame(this.config.nbReducer, 600)

  // stores the last known contribution of reducers : maps that associate reducer name (number) to (resp.) todo (list)/current tasks/done (list)
  private var taskMap = Map.empty[String, List[Long]]
  private var currentPerformedTaskMap = Map.empty[String, Long]
  private var doneTaskMap = Map.empty[String, List[Long]]

  def taskInitialized(taskEvent: TaskEvent) {
    val key = taskEvent.reducerName.substring(Monitor.reducerNamePrefix.length())

    taskMap(key) = (taskEvent.taskList map { _.nbValues })
    this.frame.updateData(taskMap, doneTaskMap, currentPerformedTaskMap)
  }

  def taskRemoved(taskEvent: TaskEvent) {
    val key = taskEvent.reducerName.substring(Monitor.reducerNamePrefix.length())

    taskMap.update(key, (taskEvent.taskList map { _.nbValues }))
    this.frame.updateData(taskMap, doneTaskMap, currentPerformedTaskMap)
  }

  def taskAdded(taskEvent: TaskEvent) {
    val key = taskEvent.reducerName.substring(Monitor.reducerNamePrefix.length())
    taskMap.update(key, (taskEvent.taskList map { _.nbValues }))
    this.frame.updateData(taskMap, doneTaskMap, currentPerformedTaskMap)
  }

  def taskDone(taskEvent: TaskEvent) {
    val key = taskEvent.reducerName.substring(Monitor.reducerNamePrefix.length())
    val performedTask = currentPerformedTaskMap getOrElse (key, 0.toLong)
    doneTaskMap.update(key, (doneTaskMap getOrElse (key, List.empty[Long])) ::: List(performedTask))
    currentPerformedTaskMap(key) = 0
    this.frame.updateData(taskMap, doneTaskMap, currentPerformedTaskMap)
  }

  def taskPerformed(taskEvent: TaskEvent) {
    val key = taskEvent.reducerName.substring(Monitor.reducerNamePrefix.length())
    currentPerformedTaskMap(key) = taskEvent.task.nbValues
    this.frame.updateData(taskMap, doneTaskMap, currentPerformedTaskMap)
  }
}

object BarChartDisplay {

  lazy val config = ConfigurationBuilder.config

  val PADDING = 20
  val TASK_WIDTH = 20
  val TASK_PADDING = 10
  val X_AXIS_SHIFT = 30
  val Y_AXIS_SHIFT = 60
  val DEFAULT_TASK_SCALE = this.config.monitorTaskScale
  val DEFAULT_TASK_SCALE_STEP = this.config.monitorTaskScaleStep
  val MIN_TASK_HEIGHT = 2
  val NUMBER_OF_GRADATIONS = 10
  val OVERHANG = 5
  val DONE_TASK_COLOR =  Color.green
  val CURRENT_TASK_COLOR = Color.black
  val TODO_TASK_COLOR = Color.red

}

/**
 * the component that displaid the tasks, using <code>paint</code> method
 *
 * done, current and todo tasks are separated
 */
class BarChartDisplay(myWidth: Int, myHeight: Int) extends JComponent {

  import BarChartDisplay._

  val zero = this.myHeight - X_AXIS_SHIFT
  // corresponds to the task cost represented by 1px on the screen
  var taskScale: Double = DEFAULT_TASK_SCALE

  // maps that associate reducer name (number) to (resp.) todo (list)/done (list)/current tasks
  var dataTasks : Map[String, List[Int]] = Map.empty[String, List[Int]]
  var performedTasks : Map[String, List[Int]] = Map.empty[String, List[Int]]
  var currentTasks: Map[String, Int] = Map.empty[String, Int]

  override def paint(g: Graphics) = {
    val g2d: Graphics2D = g.asInstanceOf[Graphics2D]
    this.drawAxis(g2d)
    this.drawAllTasks(g2d)
  }

  def setData(data: Map[String, List[Long]], performedData: Map[String, List[Long]], currentPerformedTask: Map[String, Long]) = {
    this.dataTasks = data map {
      case (key, list) => key -> (list map (_.toInt))
    }
    this.performedTasks = performedData map {
      case (key, list) => key -> (list map (_.toInt))
    }
    this.currentTasks = currentPerformedTask map {
      case (key, x) => key -> x.toInt
    }
  }

  def setTaskScale(value: Double) {
    this.taskScale = value
  }

  //
  def drawAxis(g: Graphics2D) = {
    val verticalSize = this.myHeight - 2 * X_AXIS_SHIFT
    g.drawLine(Y_AXIS_SHIFT, this.zero + OVERHANG, Y_AXIS_SHIFT, this.zero - verticalSize - OVERHANG)
    val scale = taskScale.toInt
    val gradation = verticalSize / NUMBER_OF_GRADATIONS
    for (i <- 1 until NUMBER_OF_GRADATIONS + 1) {
      g.setPaint(Color.black)
      g.drawString((gradation * i * scale).toString, OVERHANG, this.zero - i * gradation)
      g.setPaint(new Color(160, 160, 160))
      val dashed: Stroke = new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 0, Array(9), 0);
      g.setStroke(dashed);
      g.drawLine(Y_AXIS_SHIFT - OVERHANG, this.zero - i * gradation, this.myWidth - PADDING + TASK_PADDING + OVERHANG, this.zero - i * gradation)
    }
    g.setStroke(new BasicStroke)
    g.setPaint(Color.black)
    g.drawLine(Y_AXIS_SHIFT - OVERHANG, this.zero + 1, this.myWidth - PADDING + TASK_PADDING + 2*OVERHANG, this.zero + 1)

  }

  // draw the tasks of every reducer
  private def drawAllTasks(g: Graphics2D) {
    this.dataTasks foreach {
      case (key, value) => this.drawTasks(g, key, key.toInt, this.performedTasks getOrElse (key, List.empty[Int]), value, this.currentTasks getOrElse (key, 0))
    }
  }

  // draw the task of the given reducer. Each task is a rectangle whose height is proportional to task cost but not less than MIN_TASK_HEIGHT.
  // The border color depends on task status (done, todo or current).
  private def drawTasks(g: Graphics2D, reducer: String, i: Int, performedTasks: List[Int], tasks: List[Int], currentTask: Int) = {
    val xShift = Y_AXIS_SHIFT + 2 * TASK_PADDING + (i - 1) * (TASK_WIDTH + TASK_PADDING)
    g.setPaint(Color.black)
    g.drawString(reducer, xShift - 5, this.zero + 20)

    var yShift = 0
    performedTasks foreach (task => {
      val height = Math.max(Math.ceil(task / taskScale), MIN_TASK_HEIGHT).toInt
      this.drawTask(g, height, xShift - TASK_WIDTH / 2, this.zero - yShift, DONE_TASK_COLOR)
      yShift = yShift + height + strokeWidth
    })
    //
    if (currentTask != 0) {
      val height = Math.max(Math.ceil(currentTask / taskScale), MIN_TASK_HEIGHT).toInt
      this.drawTask(g, height, xShift - TASK_WIDTH / 2, this.zero - yShift, CURRENT_TASK_COLOR)
      yShift = yShift + height + strokeWidth
    }
    //
    tasks foreach (task => {
      val height = Math.max(Math.ceil(task / taskScale), MIN_TASK_HEIGHT).toInt
      this.drawTask(g, height, xShift - TASK_WIDTH / 2, this.zero - yShift, TODO_TASK_COLOR)
      yShift = yShift + height + strokeWidth
    })
  }

  val strokeWidth = 1
  // draw a task as a rectangle
  private def drawTask(g: Graphics2D, height: Int, x: Int, y: Int, color: Color) = {
    g.setStroke(new BasicStroke(strokeWidth));
    g.setPaint(color)
    g.drawRect(x, y - height , TASK_WIDTH, height)
    g.setPaint(getColor(height, x))
    g.fillRect(x + 1, y - height + 1, TASK_WIDTH - strokeWidth , height - 1)
  }

  private def getColor(height: Int, x: Int): Color = {
    new Color((height * 16 + x) % 255, (height * 32 + x) % 255, (height * 64 + x) % 255)
  }

  val alea = new Random()
  private def randomColor(): Color = {
    new Color(alea.nextInt(255), alea.nextInt(255), alea.nextInt(255))
  }
}

/**
 * frame that displays the evolution of reducer's tasks
  */
class MyFrame(nbReducers: Int, myHeight: Int) extends JFrame {

  import BarChartDisplay._

  val jf = new JFrame()
  val myWidth = nbReducers * (TASK_WIDTH + TASK_PADDING) + PADDING * 2 + Y_AXIS_SHIFT

  this.jf.addWindowListener(new CloseWindowEvent())
  this.jf.setSize(myWidth + 50, myHeight + 100)

  val myPanel = new JPanel();
  this.jf.setContentPane(this.myPanel);
  this.myPanel.setBackground(new Color(220, 220, 220))

  val barChart = new BarChartDisplay(myWidth, myHeight);
  val zoomIn = new JButton("+")
  val zoomOut = new JButton("-")
  val taskScale = new JTextField(DEFAULT_TASK_SCALE.toString, 6)
  val taskScaleStep = new JTextField(DEFAULT_TASK_SCALE_STEP.toString, 4)

  this.init()
  this.jf.setVisible(true)

  def init() = {
    this.myPanel.setLayout(new BorderLayout())
    this.myPanel.add(this.barChart, BorderLayout.CENTER)
    val northpanel = new JPanel()
    northpanel.add(new JLabel("   1px corresponds to a task cost of : "))
    northpanel.add(zoomOut)
    northpanel.add(taskScale)
    northpanel.add(zoomIn)
    northpanel.add(new JLabel(" step : "))
    northpanel.add(taskScaleStep)
    zoomIn.addActionListener(new ZoomListener(1))
    zoomOut.addActionListener(new ZoomListener(-1))
    taskScale.setHorizontalAlignment(SwingConstants.RIGHT);
    taskScale.addActionListener(new TaskScaleListener())
    this.myPanel.add(northpanel, BorderLayout.NORTH)
  }

  def updateData(data: Map[String, List[Long]], performedData: Map[String, List[Long]], currentPerformedTask: Map[String, Long]) = {
    this.barChart.setData(data, performedData, currentPerformedTask);
    this.barChart.repaint()
  }

  //
  class ZoomListener(zoomFactor: Int) extends ActionListener {
    def actionPerformed(e: ActionEvent) = {
      val value = Math.max(1, taskScale.getText().toInt + taskScaleStep.getText.toInt * this.zoomFactor)
      taskScale.setText(value.toString)
      barChart.setTaskScale(value)
      barChart.repaint()
    }
  }
  //
  class TaskScaleListener extends ActionListener {
    def actionPerformed(e: ActionEvent) {
      try {
        val value = taskScale.getText().toDouble
        barChart.setTaskScale(value)
        barChart.repaint()
      } catch {
        case _: Exception => taskScale.setText(DEFAULT_TASK_SCALE.toString())
      }
    }
  }
  //
  class CloseWindowEvent extends WindowAdapter {
    override def windowClosing(e: java.awt.event.WindowEvent) = {
      jf.dispose()
    }
  } // CloseWindowEvent

}
